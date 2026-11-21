//! The picture, sound, screenshots and recording, through GStreamer: one pipeline while a card is open.
//!
//!   v4l2src (MJPEG or YUYV at the mode you chose)
//!     → MJPEG decoded by the graphics card (vajpegdec + vapostproc, VA-API) where it can, else jpegdec (the CPU)
//!     → tee → [the window: RGBA into an appsink, at most one frame waiting: Low delay drops late frames]
//!            → [while recording: H.264 (vah264enc, else openh264enc / x264enc) into MP4]
//!   the card's USB microphone (pipewiresrc) → tee → volume (mute) → pipewiresink (this PC's speakers)
//!                                                  → [while recording: AAC (or Opus) into the same MP4]
//!
//! The delay readout is measured on every frame: from when the card's frame reached this PC (v4l2's own capture time
//! on the pipeline's clock) to when the window takes it, plus one screen refresh (it's on screen at the next one).
//!
//! No signal: no frames for 2 s, or nothing but a blank (one-colour) picture from the start (1.5 s), or for 10 s
//! after a real picture (a game's black loading screen is shorter). Then the window waits with the card's lightest
//! mode (a few small frames a second, or none at all when the card sends nothing) and comes back by itself on the
//! first real picture. Nothing here polls: threads sleep in GStreamer's bus or on a channel.
//!
//! CI's stand-in card (card.rs fake_picture): an appsrc sends its JPEG as the card's MJPEG at the mode's frame rate
//! (a thread reads it again when it changes, so CI can make the signal arrive), and `audiotestsrc` stands in for the microphone; the rest of the pipeline is the same.

use crate::card::Card;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The pipeline is running: what it asked the card for and how it decodes ("graphics card" / "CPU").
    Started { format: String, w: u32, h: u32, fps: u32, decoder: String, watching: bool },
    /// A new frame is waiting (frame()).
    Frame,
    /// A picture is coming in (true) or not (false).
    Signal(bool),
    /// It stopped by itself (the card was unplugged, or GStreamer failed), in plain words.
    Failed(String),
    Recording(Result<PathBuf, String>),
    /// A recording was finished and closed (its file), or couldn't be.
    Recorded(Result<PathBuf, String>),
}

#[derive(Debug, Clone)]
pub struct Ask {
    pub card: Card,
    pub format: String,
    pub w: u32,
    pub h: u32,
    pub fps: u32,
    pub low_delay: bool,
    /// The card's sound (a PipeWire source), if it has one.
    pub sound: Option<String>,
    pub play_sound: bool,
    pub volume: u32,
    pub muted: bool,
    /// Waiting for a signal: the card's lightest mode, and Signal(true) on the first real picture.
    pub watching: bool,
}

pub struct Frame {
    pub w: u32,
    pub h: u32,
    pub rgba: Vec<u8>,
    /// From the card to here (ns), and when it was put here.
    pub delay_ns: u64,
    pub at: Instant,
}

static EVENTS: OnceLock<(UnboundedSender<(u64, Event)>, Mutex<Option<UnboundedReceiver<(u64, Event)>>>)> = OnceLock::new();
static GEN: AtomicU64 = AtomicU64::new(0);
static FRAME: Mutex<Option<Frame>> = Mutex::new(None);
static PENDING: AtomicBool = AtomicBool::new(false);

fn bus() -> &'static (UnboundedSender<(u64, Event)>, Mutex<Option<UnboundedReceiver<(u64, Event)>>>) {
    EVENTS.get_or_init(|| {
        let (tx, rx) = unbounded();
        (tx, Mutex::new(Some(rx)))
    })
}

fn send(gen: u64, e: Event) {
    let _ = bus().0.unbounded_send((gen, e));
}

/// The window's subscription: the running pipeline's events (nothing when none runs). Each carries its pipeline's
/// number, so an old pipeline's last words are told apart from a new one's.
pub fn events() -> impl futures::Stream<Item = (u64, Event)> {
    let rx = bus().1.lock().unwrap().take();
    match rx {
        Some(r) => futures::stream::StreamExt::boxed(r),
        None => futures::stream::StreamExt::boxed(futures::stream::pending()),
    }
}

/// The number of the pipeline started last.
pub fn current() -> u64 {
    GEN.load(Ordering::Relaxed)
}

/// The newest frame (the window shows it, then waits for the next Frame event).
pub fn frame() -> Option<Frame> {
    PENDING.store(false, Ordering::Release);
    FRAME.lock().unwrap().take()
}

/// Whether the stand-in card is in use (CI).
pub fn stand_in(card: &Card) -> bool {
    card.fake_picture.is_some()
}

/// Is a blank picture (one colour all over: what cards send without HDMI)? A grid of 8 × 8 points.
pub fn blank(rgba: &[u8], w: u32, h: u32) -> bool {
    if w == 0 || h == 0 || rgba.len() < (w * h * 4) as usize {
        return true;
    }
    let (mut lo, mut hi) = ([255u8; 3], [0u8; 3]);
    for gy in 0..8u32 {
        for gx in 0..8u32 {
            let x = (w - 1) * (2 * gx + 1) / 16;
            let y = (h - 1) * (2 * gy + 1) / 16;
            let i = ((y * w + x) * 4) as usize;
            for c in 0..3 {
                lo[c] = lo[c].min(rgba[i + c]);
                hi[c] = hi[c].max(rgba[i + c]);
            }
        }
    }
    (0..3).all(|c| hi[c].saturating_sub(lo[c]) < 10)
}

/// Which decoder: "va" (the graphics card) or "cpu". HEARTH_CAPTURE_DECODER picks one; a VA decoder that failed
/// once isn't tried again until the app opens again.
pub static VA_FAILED: AtomicBool = AtomicBool::new(false);

/// The pipeline's description (also checked by `cargo test`). `va`: decode with the graphics card.
pub fn describe(ask: &Ask, va: bool, have_pw: bool) -> String {
    let mut d = describe_video(ask, va);
    if let Some(a) = describe_sound(ask, have_pw) {
        d.push(' ');
        d.push_str(&a);
    }
    d
}

fn describe_parts(ask: &Ask, va: bool) -> (String,) {
    let fake = ask.card.fake_picture.as_ref();
    let (fmt, w, h, fps) = if ask.watching { ask.card.lightest().unwrap_or(("MJPG".into(), ask.w, ask.h, 5)) } else { (ask.format.clone(), ask.w, ask.h, ask.fps) };
    let mjpg = fmt == "MJPG" || fake.is_some();
    let src = match fake {
        Some(_) => format!("appsrc name=src is-live=true do-timestamp=true format=time caps=\"image/jpeg,framerate={fps}/1\" ! jpegparse"),
        None if mjpg => format!("v4l2src name=src device=\"{}\" ! image/jpeg,width={w},height={h},framerate={fps}/1", ask.card.dev.display()),
        None => format!("v4l2src name=src device=\"{}\" ! video/x-raw,format=YUY2,width={w},height={h},framerate={fps}/1", ask.card.dev.display()),
    };
    let dec = match (mjpg, va) {
        (true, true) => "vajpegdec name=dec",
        (true, false) => "jpegdec name=dec",
        (false, true) => "identity name=dec",
        (false, false) => "identity name=dec",
    };
    let to_rgba = if va { "vapostproc ! video/x-raw,format=RGBA" } else { "videoconvert n-threads=2 ! video/x-raw,format=RGBA" };
    let view = if ask.low_delay {
        format!("vt. ! queue name=vq max-size-buffers=1 max-size-bytes=0 max-size-time=0 leaky=downstream ! {to_rgba} ! appsink name=pic sync=false max-buffers=1 drop=true")
    } else {
        format!("vt. ! queue name=vq max-size-buffers=4 max-size-bytes=0 max-size-time=0 ! {to_rgba} ! appsink name=pic sync=true max-buffers=2 drop=false")
    };
    (format!("{src} ! {dec} ! tee name=vt allow-not-linked=true {view}"),)
}

/// The picture's part alone: what's started first (the owner's Elgato 4K S, Nov 21: opening its sound at the same
/// moment as its picture left the picture black; OBS opens the picture alone and it works).
pub fn describe_video(ask: &Ask, va: bool) -> String {
    describe_parts(ask, va).0
}

/// The card's sound: its own pipeline (never part of the picture's), started half a second after the first frame:
/// the speakers, and an appsink that hands recordings the sound. None when there's no sound.
/// The sound's format between the sound's pipeline and a recording.
pub const SOUND_CAPS: &str = "audio/x-raw,format=S16LE,layout=interleaved,rate=48000,channels=2";

pub fn describe_sound(ask: &Ask, have_pw: bool) -> Option<String> {
    let fake = ask.card.fake_picture.as_ref();
    let mut d = String::new();
    if !ask.watching {
        if let Some(s) = &ask.sound {
            let asrc = if fake.is_some() {
                "audiotestsrc name=asrc is-live=true wave=sine volume=0.05".to_string()
            } else if have_pw {
                format!("pipewiresrc name=asrc target-object=\"{}\" do-timestamp=true stream-properties=\"props,node.latency=(string)1024/48000\"", s.replace('"', ""))
            } else {
                format!("pulsesrc name=asrc device=\"{}\" do-timestamp=true buffer-time=100000 latency-time=20000", s.replace('"', ""))
            };
            d.push_str(&format!("{asrc} ! audioconvert ! audioresample ! {SOUND_CAPS} ! queue max-size-time=200000000 max-size-buffers=0 max-size-bytes=0 leaky=downstream ! tee name=at allow-not-linked=true at. ! queue max-size-time=200000000 max-size-buffers=0 max-size-bytes=0 leaky=downstream ! appsink name=recsink sync=false max-buffers=100 drop=true"));
            if ask.play_sound {
                let sink = if fake.is_some() { "fakesink name=asink sync=true" } else if have_pw { "pipewiresink name=asink sync=false stream-properties=\"props,node.latency=(string)1024/48000\"" } else { "pulsesink name=asink sync=false buffer-time=100000 latency-time=20000" };
                d.push_str(&format!(" at. ! queue max-size-time=200000000 max-size-buffers=0 max-size-bytes=0 leaky=downstream ! volume name=vol volume={:.2} mute={} ! {sink}", ask.volume.min(100) as f64 / 100.0, ask.muted));
            }
        }
    }
    if d.is_empty() { None } else { Some(d) }
}

/// The recording's own part of the pipeline (added while it runs): H.264 and the sound into MP4.
pub fn describe_rec(file: &Path, va_decoded: bool, venc: &str, aenc: Option<&str>) -> String {
    let mut conv = String::new();
    if va_decoded {
        conv.push_str("vapostproc ! video/x-raw,format=NV12 ! ");
    }
    let enc = match venc {
        "vah264enc" | "vah264lpenc" => format!("{}videoconvert ! video/x-raw,format=NV12 ! {venc} bitrate=30000 key-int-max=120", conv),
        "openh264enc" => format!("{}videoconvert ! video/x-raw,format=I420 ! openh264enc bitrate=30000000 rate-control=bitrate complexity=low", conv),
        _ => format!("{}videoconvert ! video/x-raw,format=I420 ! x264enc bitrate=30000 speed-preset=veryfast tune=zerolatency key-int-max=120", conv),
    };
    let mut d = format!(
        "queue name=rvq max-size-buffers=0 max-size-bytes=0 max-size-time=3000000000 ! {enc} ! h264parse ! queue ! mp4mux name=mux fragment-duration=1000 ! filesink name=rfile location=\"{}\"",
        file.display().to_string().replace('"', "")
    );
    if let Some(a) = aenc {
        let ae = match a {
            "fdkaacenc" => "fdkaacenc bitrate=192000 ! aacparse",
            "avenc_aac" => "avenc_aac bitrate=192000 ! aacparse",
            "voaacenc" => "voaacenc bitrate=192000 ! aacparse",
            _ => "opusenc bitrate=192000 ! opusparse",
        };
        d.push_str(&format!(" appsrc name=raudio is-live=true format=time do-timestamp=true caps=\"{SOUND_CAPS}\" ! queue name=raq max-size-buffers=0 max-size-bytes=0 max-size-time=3000000000 ! audioconvert ! audioresample ! {ae} ! queue ! mux."));
    }
    d
}

/// What a recording or its thumbnail needs to know about a file (Recordings & screenshots).
pub struct Probe {
    pub secs: u64,
    pub w: u32,
    pub h: u32,
    pub fps: u32,
    pub tw: u32,
    pub th: u32,
    pub rgba: Vec<u8>,
}

#[cfg(not(feature = "media"))]
mod imp {
    use super::*;
    pub fn start(_ask: Ask) {
        let g = GEN.fetch_add(1, Ordering::Relaxed) + 1;
        send(g, Event::Failed("This copy of Capture Card was built without its player (GStreamer).".into()));
    }
    pub fn stop() {}
    pub fn set_volume(_v: u32, _muted: bool) {}
    pub fn restart_sound(_ask: Ask) {}
    pub fn running() -> bool {
        false
    }
    pub fn record_start(_f: PathBuf) {}
    pub fn record_stop() {}
    pub fn recording() -> Option<(Instant, PathBuf)> {
        None
    }
    pub fn probe(_p: &Path) -> Option<Probe> {
        None
    }
    pub fn encoders() -> (Option<String>, Option<String>) {
        (None, None)
    }
}

#[cfg(feature = "media")]
mod imp {
    use super::*;
    use gstreamer as gst;
    use gstreamer::prelude::*;
    use std::sync::Arc;
    use std::time::Duration;

    struct Running {
        gen: u64,
        pipeline: gst::Pipeline,
        va: bool,
        rec: Option<Rec>,
        /// A frame came in (the start tries the next way when none does within a few seconds).
        first: Arc<AtomicBool>,
        /// The start stopped trying other ways: from now on no frames for 2 s is "no signal".
        settled: Arc<AtomicBool>,
    }

    /// Takes a pipeline down, waiting at most 3 s: a card or decoder that hangs never holds up the window or the next
    /// try (the owner's Elgato 4K S, Nov 21: "just freezing now"). True when it let go in time.
    fn teardown(p: gst::Pipeline, why: &str) -> bool {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let t0 = Instant::now();
        let _ = std::thread::Builder::new().name("cc-null".into()).spawn(move || {
            let _ = p.set_state(gst::State::Null);
            let _ = tx.send(());
        });
        let ok = rx.recv_timeout(Duration::from_secs(3)).is_ok();
        crate::log(&format!("stopped ({why}): {}", if ok { format!("in {} ms", t0.elapsed().as_millis()) } else { "the card didn't let go within 3 s; going on without waiting".into() }));
        ok
    }

    /// The ways to try, best first: what you chose (with the graphics card's decoder where it can), the same with the
    /// CPU's, your size at the card's own best rate (an HDMI signal at 60 gives nothing at 30 on some cards), then
    /// YUYV 1080p60. Waiting for a signal: only the lightest mode.
    fn ladder(ask: &Ask) -> Vec<(Ask, bool)> {
        let va = use_va(ask);
        if ask.watching || ask.card.fake_picture.is_some() {
            return vec![(ask.clone(), va)];
        }
        let mut out: Vec<(Ask, bool)> = Vec::new();
        let mut add = |a: Ask, v: bool, out: &mut Vec<(Ask, bool)>| {
            if !out.iter().any(|(b, w)| b.format == a.format && b.w == a.w && b.h == a.h && b.fps == a.fps && *w == v) {
                out.push((a, v));
            }
        };
        if va {
            add(ask.clone(), true, &mut out);
        }
        add(ask.clone(), false, &mut out);
        let rates = ask.card.rates(&ask.format, ask.w, ask.h);
        let best = if rates.contains(&60) { Some(60) } else { rates.iter().copied().filter(|r| *r <= 60).max() };
        if let Some(r) = best.filter(|r| *r != ask.fps) {
            add(Ask { fps: r, ..ask.clone() }, false, &mut out);
        }
        if let Some((f, w, h, r)) = ask.card.pick("YUYV", 1920, 1080, 60).filter(|m| m.0 == "YUYV") {
            add(Ask { format: f, w, h, fps: r, ..ask.clone() }, false, &mut out);
        }
        out
    }

    struct Rec {
        bin: gst::Bin,
        pads: Vec<(gst::Element, gst::Pad)>,
        file: PathBuf,
        since: Instant,
    }

    static RUN: Mutex<Option<Running>> = Mutex::new(None);
    /// The card's sound, its own pipeline (Nov 21: the Elgato 4K S's picture stayed black, and the picture couldn't
    /// stop, with the sound in the same pipeline).
    static SOUND: Mutex<Option<gst::Pipeline>> = Mutex::new(None);
    /// The recording's sound input, while one runs: the sound's pipeline hands it each piece.
    static REC_AUDIO: Mutex<Option<gstreamer_app::AppSrc>> = Mutex::new(None);

    fn stop_sound() {
        let p = SOUND.lock().unwrap().take();
        if let Some(p) = p {
            let _ = std::thread::Builder::new().name("cc-sound-stop".into()).spawn(move || {
                teardown(p, "the sound");
            });
        }
    }

    fn start_sound(sd: &str) {
        stop_sound();
        let p = match gst::parse::launch(sd).map(|e| e.downcast::<gst::Pipeline>()) {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => return crate::log("the card's sound couldn't be set up; the picture goes on without it"),
            Err(e) => return crate::log(&format!("the card's sound couldn't be set up ({e}); the picture goes on without it")),
        };
        if let Some(sink) = p.by_name("recsink").and_then(|e| e.downcast::<gstreamer_app::AppSink>().ok()) {
            sink.set_callbacks(
                gstreamer_app::AppSinkCallbacks::builder()
                    .new_sample(|s| {
                        let sample = s.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                        if let Some(src) = REC_AUDIO.lock().unwrap().as_ref() {
                            if let Some(b) = sample.buffer_owned() {
                                let mut b = b;
                                {
                                    let bm = b.make_mut();
                                    bm.set_pts(gst::ClockTime::NONE);
                                    bm.set_dts(gst::ClockTime::NONE);
                                }
                                let _ = src.push_buffer(b);
                            }
                        }
                        Ok(gst::FlowSuccess::Ok)
                    })
                    .build(),
            );
        }
        if p.set_state(gst::State::Playing).is_err() {
            crate::log("the card's sound didn't start; the picture goes on without it");
            let _ = std::thread::Builder::new().name("cc-sound-stop".into()).spawn(move || {
                teardown(p, "the sound didn't start");
            });
            return;
        }
        crate::log("sound on (its own pipeline)");
        *SOUND.lock().unwrap() = Some(p);
    }

    /// The sound alone again (a sound setting changed): the picture and the card aren't touched.
    pub fn restart_sound(ask: Ask) {
        match describe_sound(&ask, use_pw()) {
            Some(sd) if RUN.lock().unwrap().is_some() => {
                let _ = std::thread::Builder::new().name("cc-sound".into()).spawn(move || start_sound(&sd));
            }
            _ => stop_sound(),
        }
    }

    fn init() -> bool {
        static OK: OnceLock<bool> = OnceLock::new();
        *OK.get_or_init(|| gst::init().is_ok())
    }

    fn have(f: &str) -> bool {
        gst::ElementFactory::find(f).is_some()
    }

    /// The sound goes through PipeWire's Pulse side (pulsesrc / pulsesink, what OBS uses) whenever it's there:
    /// with pipewiresrc and pipewiresink in one pipeline the Elgato 4K S was linked but nothing came through, and
    /// the sound's stop hung (Nov 21). pipewiresrc only when there's no pulse plugin.
    fn use_pw() -> bool {
        !(have("pulsesrc") && have("pulsesink")) && have("pipewiresrc")
    }

    /// The encoders this PC has for recording: (video, sound).
    pub fn encoders() -> (Option<String>, Option<String>) {
        if !init() {
            return (None, None);
        }
        let v = ["vah264enc", "vah264lpenc", "openh264enc", "x264enc"].into_iter().find(|f| have(f)).map(String::from);
        let a = ["fdkaacenc", "avenc_aac", "voaacenc", "opusenc"].into_iter().find(|f| have(f)).map(String::from);
        (v, a)
    }

    fn use_va(ask: &Ask) -> bool {
        match std::env::var("HEARTH_CAPTURE_DECODER").as_deref() {
            Ok("cpu") => return false,
            Ok("va") => return have("vapostproc"),
            _ => {}
        }
        if VA_FAILED.load(Ordering::Relaxed) || !have("vapostproc") {
            return false;
        }
        let mjpg = ask.format == "MJPG" || ask.card.fake_picture.is_some() || ask.watching;
        // MJPEG needs vajpegdec; YUYV only the converter.
        !mjpg || have("vajpegdec")
    }

    pub fn running() -> bool {
        RUN.lock().unwrap().is_some() || STARTING.load(Ordering::Relaxed)
    }

    /// A start is still trying its ways (between them nothing is running, and that's not "stopped").
    static STARTING: AtomicBool = AtomicBool::new(false);

    /// Held while a pipeline is being taken down or set up, so a new one never meets the old one at the card.
    static TURN: Mutex<()> = Mutex::new(());

    /// The running picture, taken out (the stop's own message posted to its bus), for whoever takes it down.
    fn take_running() -> Option<Running> {
        PENDING.store(false, Ordering::Release);
        let r = RUN.lock().unwrap().take();
        *FRAME.lock().unwrap() = None;
        if let Some(r) = &r {
            if let Some(b) = r.pipeline.bus() {
                let _ = b.post(gst::message::Application::new(gst::Structure::new_empty("hearth-stop")));
            }
        }
        r
    }

    pub fn stop() {
        GEN.fetch_add(1, Ordering::Relaxed);
        STARTING.store(false, Ordering::Relaxed);
        stop_sound();
        // A frame the window was told of but never took must not hold up the next pipeline's.
        let r = take_running();
        if let Some(r) = r {
            // Taken down off the window's thread (closing the card can take a moment).
            let _ = std::thread::Builder::new().name("cc-stop".into()).spawn(move || {
                let _turn = TURN.lock().unwrap();
                if let Some(rec) = r.rec {
                    // Stopping with a recording open: its file is closed properly first.
                    finish_rec(&r.pipeline, rec, r.gen, true);
                }
                teardown(r.pipeline, "stop");
            });
        }
    }

    #[cfg(test)]
    mod ladder_tests {
        use super::*;
        use crate::card::{Card, Mode};

        fn elgato() -> Card {
            Card {
                dev: "/dev/video2".into(),
                id: "0fd9:00af:x".into(),
                name: "Elgato 4K S".into(),
                chip: String::new(),
                speed: 5000,
                usb_port: "1-1".into(),
                modes: vec![
                    Mode { format: "YUYV".into(), w: 1920, h: 1080, fps: vec![60, 50, 30, 25] },
                    Mode { format: "MJPG".into(), w: 1920, h: 1080, fps: vec![240, 144, 120, 60, 50, 30, 25] },
                ],
                fake_picture: None,
                fake_sound: None,
            }
        }

        #[test]
        fn an_elgato_at_30_tries_the_cpu_then_60_then_yuyv() {
            assert!(init());
            std::env::set_var("HEARTH_CAPTURE_DECODER", "cpu");
            let ask = Ask { card: elgato(), format: "MJPG".into(), w: 1920, h: 1080, fps: 30, low_delay: true, sound: None, play_sound: false, volume: 70, muted: false, watching: false };
            let l = ladder(&ask);
            let ways: Vec<String> = l.iter().map(|(a, v)| format!("{} {}x{}@{} {}", a.format, a.w, a.h, a.fps, if *v { "va" } else { "cpu" })).collect();
            assert_eq!(ways, vec!["MJPG 1920x1080@30 cpu", "MJPG 1920x1080@60 cpu", "YUYV 1920x1080@60 cpu"]);
            let w = ladder(&Ask { watching: true, ..ask });
            assert_eq!(w.len(), 1, "waiting for a signal: only the lightest mode");
        }
    }

    pub fn set_volume(v: u32, muted: bool) {
        let p = SOUND.lock().unwrap().clone();
        if let Some(p) = p {
            if let Some(vol) = p.by_name("vol") {
                vol.set_property("volume", v.min(100) as f64 / 100.0);
                vol.set_property("mute", muted);
            }
        }
    }

    pub fn start(ask: Ask) {
        // The old picture is taken down by this start itself, before it opens the card (a separate stop could still
        // hold the card when the new one opened it: "Another app is using the capture card", Nov 21).
        let old = take_running();
        stop_sound();
        let gen = GEN.fetch_add(1, Ordering::Relaxed) + 1;
        // Set up off the window's thread (opening the card and the first moments take a little while).
        STARTING.store(true, Ordering::Relaxed);
        let _ = std::thread::Builder::new().name("cc-start".into()).spawn(move || {
            let _turn = TURN.lock().unwrap();
            if let Some(r) = old {
                if let Some(rec) = r.rec {
                    finish_rec(&r.pipeline, rec, r.gen, true);
                }
                teardown(r.pipeline, "a new start");
            }
            struct Done(u64);
            impl Drop for Done {
                fn drop(&mut self) {
                    if self.0 == current() {
                        STARTING.store(false, Ordering::Relaxed);
                    }
                }
            }
            let _done = Done(gen);
            if gen != current() {
                return;
            }
            if !init() {
                send(gen, Event::Failed("GStreamer couldn't start on this PC.".into()));
                return;
            }
            let tries = ladder(&ask);
            let n = tries.len();
            let mut last_err = String::new();
            for (i, (a, va)) in tries.into_iter().enumerate() {
                if gen != current() {
                    return;
                }
                let last = i + 1 == n;
                crate::log(&format!("try {}/{}: {} {}x{} at {} with the {}", i + 1, n, a.format, a.w, a.h, a.fps, if va { "graphics card's decoder" } else { "CPU's decoder" }));
                let r = match build(&a, va, gen) {
                    Ok(r) => r,
                    Err(e) => {
                        crate::log(&format!("didn't start: {e}"));
                        if va {
                            VA_FAILED.store(true, Ordering::Relaxed);
                        }
                        last_err = e;
                        continue;
                    }
                };
                let (first, settled) = (r.first.clone(), r.settled.clone());
                {
                    let mut g = RUN.lock().unwrap();
                    if gen != current() {
                        drop(g);
                        teardown(r.pipeline, "a newer start");
                        return;
                    }
                    *g = Some(r);
                }
                if last {
                    settled.store(true, Ordering::Relaxed);
                    return;
                }
                // A picture within 3 s: this way works. None: the next way (this one let go first).
                let t0 = Instant::now();
                while !first.load(Ordering::Relaxed) && t0.elapsed() < Duration::from_secs(3) && gen == current() {
                    std::thread::sleep(Duration::from_millis(50));
                }
                if first.load(Ordering::Relaxed) {
                    crate::log(&format!("picture after {} ms", t0.elapsed().as_millis()));
                    settled.store(true, Ordering::Relaxed);
                    return;
                }
                if gen != current() {
                    return;
                }
                crate::log("no picture within 3 s");
                if va {
                    VA_FAILED.store(true, Ordering::Relaxed);
                }
                let taken = RUN.lock().unwrap().take();
                if let Some(r) = taken {
                    teardown(r.pipeline, "trying the next way");
                }
            }
            if !last_err.is_empty() && gen == current() {
                send(gen, Event::Failed(last_err));
            }
        });
    }

    fn build(ask: &Ask, va: bool, gen: u64) -> Result<Running, String> {
        let desc = describe_video(ask, va);
        let sound = describe_sound(ask, use_pw());
        crate::log(&format!("pipeline: {desc}"));
        let el = gst::parse::launch(&desc).map_err(|e| format!("The player couldn't be set up: {e}"))?;
        let pipeline = el.downcast::<gst::Pipeline>().map_err(|_| "The player couldn't be set up.".to_string())?;
        let sink = pipeline.by_name("pic").and_then(|e| e.downcast::<gstreamer_app::AppSink>().ok()).ok_or("The player has no picture.")?;
        let (beat_tx, beat_rx) = std::sync::mpsc::channel::<()>();
        let watching = ask.watching;
        let first = Arc::new(AtomicBool::new(false));
        let settled = Arc::new(AtomicBool::new(false));
        // The frames: into FRAME for the window; blank / real picture for the signal.
        {
            let started = Arc::new(AtomicBool::new(false));
            let seen_real = Arc::new(AtomicBool::new(false));
            let blank_since: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
            let signal_sent: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
            let n = Arc::new(AtomicU64::new(0));
            let fmt = if watching { ask.card.lightest().map(|m| m.0).unwrap_or_default() } else { ask.format.clone() };
            let (w0, h0, f0) = if watching { ask.card.lightest().map(|m| (m.1, m.2, m.3)).unwrap_or((ask.w, ask.h, ask.fps)) } else { (ask.w, ask.h, ask.fps) };
            let decoder = if va { "graphics card".to_string() } else { "CPU".to_string() };
            let first_cb = first.clone();
            sink.set_callbacks(
                gstreamer_app::AppSinkCallbacks::builder()
                    .new_sample(move |s| {
                        let sample = s.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                        first_cb.store(true, Ordering::Relaxed);
                        let _ = beat_tx.send(());
                        let (w, h) = sample.caps().and_then(|c| c.structure(0).map(|st| (st.get::<i32>("width").unwrap_or(0), st.get::<i32>("height").unwrap_or(0)))).unwrap_or((0, 0));
                        let Some(buf) = sample.buffer() else { return Ok(gst::FlowSuccess::Ok) };
                        let Ok(map) = buf.map_readable() else { return Ok(gst::FlowSuccess::Ok) };
                        let need = (w.max(0) * h.max(0) * 4) as usize;
                        if w <= 0 || h <= 0 || map.len() < need {
                            return Ok(gst::FlowSuccess::Ok);
                        }
                        let (w, h) = (w as u32, h as u32);
                        let k = n.fetch_add(1, Ordering::Relaxed);
                        if !started.swap(true, Ordering::Relaxed) {
                            send(gen, Event::Started { format: fmt.clone(), w: w0, h: h0, fps: f0, decoder: decoder.clone(), watching });
                        }
                        // The signal: every frame while waiting for one, every 6th otherwise.
                        if watching || k % 6 == 0 {
                            let b = blank(&map[..need], w, h);
                            let mut since = blank_since.lock().unwrap();
                            let mut sent = signal_sent.lock().unwrap();
                            if !b {
                                *since = None;
                                seen_real.store(true, Ordering::Relaxed);
                                if *sent != Some(true) {
                                    *sent = Some(true);
                                    send(gen, Event::Signal(true));
                                }
                            } else if !watching {
                                let t = *since.get_or_insert_with(Instant::now);
                                let limit = if seen_real.load(Ordering::Relaxed) { 10_000 } else { 1_500 };
                                if t.elapsed().as_millis() as u64 >= limit && *sent != Some(false) {
                                    *sent = Some(false);
                                    send(gen, Event::Signal(false));
                                }
                            }
                        }
                        if watching {
                            // Waiting for a signal: nothing is shown.
                            return Ok(gst::FlowSuccess::Ok);
                        }
                        let delay = match (s.current_running_time(), buf.pts()) {
                            (Some(now), Some(pts)) if now >= pts => (now - pts).nseconds(),
                            _ => 0,
                        };
                        *FRAME.lock().unwrap() = Some(Frame { w, h, rgba: map[..need].to_vec(), delay_ns: delay, at: Instant::now() });
                        if !PENDING.swap(true, Ordering::AcqRel) {
                            send(gen, Event::Frame);
                        }
                        Ok(gst::FlowSuccess::Ok)
                    })
                    .build(),
            );
        }
        // No frames at all for 2 s: no signal. Then it sleeps until a frame comes (or the pipeline goes).
        if !watching {
            let settled_b = settled.clone();
            let _ = std::thread::Builder::new().name("cc-beat".into()).stack_size(64 * 1024).spawn(move || {
                let mut said = false;
                loop {
                    // While the start is still trying other ways, no frames isn't "no signal" yet.
                    if !settled_b.load(Ordering::Relaxed) {
                        match beat_rx.recv_timeout(Duration::from_millis(250)) {
                            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                if gen != current() {
                                    return;
                                }
                                continue;
                            }
                            Err(_) => return,
                        }
                    }
                    let r = if said { beat_rx.recv().map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected) } else { beat_rx.recv_timeout(Duration::from_secs(2)) };
                    match r {
                        Ok(()) => said = false,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if gen != current() {
                                return;
                            }
                            said = true;
                            send(gen, Event::Signal(false));
                        }
                        Err(_) => return,
                    }
                }
            });
        } else {
            drop(beat_rx);
        }
        // Errors and the end, on the bus (a thread that sleeps there).
        let bus = pipeline.bus().ok_or("The player has no bus.")?;
        pipeline.set_state(gst::State::Playing).map_err(|_| first_error(&bus).unwrap_or_else(|| "The card couldn't be opened.".into()))?;
        // An error in the first moments (the VA decoder refusing the card's JPEGs): start again without it.
        if va {
            if let Some(m) = bus.timed_pop_filtered(gst::ClockTime::from_mseconds(1200), &[gst::MessageType::Error]) {
                let e = words(&m);
                teardown(pipeline, "the graphics card's decoder refused");
                return Err(e);
            }
        }
        // CI's stand-in card: its JPEG, sent at the mode's frame rate (read again when the file changes).
        if let Some(pic) = ask.card.fake_picture.clone() {
            if let Some(src) = pipeline.by_name("src").and_then(|e| e.downcast::<gstreamer_app::AppSrc>().ok()) {
                let fps = if watching { ask.card.lightest().map(|m| m.3).unwrap_or(5) } else { ask.fps }.max(1);
                let _ = std::thread::Builder::new().name("cc-stand-in".into()).spawn(move || {
                    let step = Duration::from_micros(1_000_000 / fps as u64);
                    let (mut seen, mut data) = (None, Vec::new());
                    let mut next = Instant::now();
                    while gen == current() {
                        let m = std::fs::metadata(&pic).and_then(|m| m.modified()).ok();
                        if m != seen {
                            if let Ok(b) = std::fs::read(&pic) {
                                data = b;
                            }
                            seen = m;
                        }
                        if !data.is_empty() && src.push_buffer(gst::Buffer::from_slice(data.clone())).is_err() {
                            break;
                        }
                        next += step;
                        let now = Instant::now();
                        if next > now {
                            std::thread::sleep(next - now);
                        } else {
                            next = now;
                        }
                    }
                });
            }
        }
        let p2 = pipeline.clone();
        let _ = std::thread::Builder::new().name("cc-bus".into()).stack_size(128 * 1024).spawn(move || loop {
            let Some(m) = bus.timed_pop_filtered(gst::ClockTime::NONE, &[gst::MessageType::Error, gst::MessageType::Eos, gst::MessageType::Application]) else { return };
            match m.view() {
                gst::MessageView::Application(a) if a.structure().map(|s| s.name().as_str() == "hearth-stop").unwrap_or(false) => return,
                gst::MessageView::Error(_) => {
                    let e = words(&m);
                    crate::log(&format!("stopped: {e}"));
                    let mine = RUN.lock().unwrap().as_ref().map(|r| r.gen == gen).unwrap_or(false);
                    if mine {
                        let _ = p2.set_state(gst::State::Null);
                        *RUN.lock().unwrap() = None;
                        send(gen, Event::Failed(e));
                    }
                    return;
                }
                gst::MessageView::Eos(_) => {
                    let mine = RUN.lock().unwrap().as_ref().map(|r| r.gen == gen).unwrap_or(false);
                    if mine {
                        let _ = p2.set_state(gst::State::Null);
                        *RUN.lock().unwrap() = None;
                        send(gen, Event::Failed("The card stopped sending.".into()));
                    }
                    return;
                }
                _ => {}
            }
        });
        // The card's sound: half a second after its first frame, never with the picture's start (some cards, like
        // the Elgato 4K S, send no picture when both open at once). Only while this pipeline is the one in use.
        if let Some(sd) = sound {
            let first3 = first.clone();
            let _ = std::thread::Builder::new().name("cc-sound".into()).stack_size(128 * 1024).spawn(move || {
                while !first3.load(Ordering::Relaxed) {
                    if gen != current() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                std::thread::sleep(Duration::from_millis(500));
                if gen != current() || RUN.lock().unwrap().as_ref().map(|r| r.gen != gen).unwrap_or(true) {
                    return;
                }
                start_sound(&sd);
            });
        }
        Ok(Running { gen, pipeline, va, rec: None, first, settled })
    }

    fn first_error(bus: &gst::Bus) -> Option<String> {
        bus.timed_pop_filtered(gst::ClockTime::from_mseconds(300), &[gst::MessageType::Error]).map(|m| words(&m))
    }

    /// An error in plain words.
    fn words(m: &gst::Message) -> String {
        if let gst::MessageView::Error(e) = m.view() {
            let raw = e.error().to_string();
            let l = raw.to_lowercase();
            let from = m.src().map(|s| s.name().to_string()).unwrap_or_default();
            crate::log(&format!("GStreamer: {raw} ({from}) {:?}", e.debug()));
            if from == "src" && (l.contains("no such device") || l.contains("disconnected") || l.contains("could not read") || l.contains("failed to")) {
                return "The capture card was unplugged or stopped answering.".into();
            }
            if from == "src" && (l.contains("busy") || l.contains("resource busy")) {
                return "Another app is using the capture card. Close it, and the picture comes up here.".into();
            }
            if from == "src" && l.contains("not negotiate") {
                return "The card can't send that picture mode. Pick another in Settings.".into();
            }
            if from == "asrc" || from == "asink" {
                return format!("The sound couldn't start: {raw}");
            }
            return format!("The picture stopped: {raw}");
        }
        "The picture stopped.".into()
    }

    pub fn recording() -> Option<(Instant, PathBuf)> {
        RUN.lock().unwrap().as_ref().and_then(|r| r.rec.as_ref().map(|x| (x.since, x.file.clone())))
    }

    /// Starts recording into `file` (the window hears Recording(Ok) or Recording(Err)).
    pub fn record_start(file: PathBuf) {
        let mut g = RUN.lock().unwrap();
        let Some(r) = g.as_mut() else {
            send(current(), Event::Recording(Err("There's no picture to record yet.".into())));
            return;
        };
        let gen = r.gen;
        if r.rec.is_some() {
            return;
        }
        let (venc, aenc) = encoders();
        let Some(venc) = venc else {
            send(gen, Event::Recording(Err("This PC can't record video: it has no H.264 encoder for GStreamer (gstreamer1-plugin-openh264 or the graphics card's VA-API).".into())));
            return;
        };
        let have_sound = SOUND.lock().unwrap().is_some();
        let desc = describe_rec(&file, r.va, &venc, if have_sound { aenc.as_deref() } else { None });
        crate::log(&format!("recording: {desc}"));
        if let Some(d) = file.parent() {
            let _ = std::fs::create_dir_all(d);
        }
        let bin = match gst::parse::bin_from_description(&desc, false) {
            Ok(b) => b,
            Err(e) => {
                crate::log(&format!("recording couldn't be set up: {e}"));
                send(gen, Event::Recording(Err(format!("Recording couldn't start: {e}"))));
                return;
            }
        };
        let mut pads = Vec::new();
        let mut why: Vec<String> = Vec::new();
        let mut ok = r.pipeline.add(&bin).is_ok();
        if !ok {
            why.push("add".into());
        }
        for (tee_name, q) in [("vt", "rvq")] {
            let (Some(tee), Some(qe)) = (r.pipeline.by_name(tee_name), bin.by_name(q)) else { continue };
            let Some(sinkpad) = qe.static_pad("sink") else { continue };
            let ghost = match gst::GhostPad::builder_with_target(&sinkpad) {
                Ok(b) => b.name(format!("in_{q}")).build(),
                Err(e) => {
                    why.push(format!("ghost {q}: {e}"));
                    ok = false;
                    continue;
                }
            };
            let ghost = ghost.upcast::<gst::Pad>();
            let _ = ghost.set_active(true);
            if let Err(e) = bin.add_pad(&ghost) {
                why.push(format!("add pad {q}: {e}"));
                ok = false;
            }
            match tee.request_pad_simple("src_%u") {
                Some(tp) => {
                    if let Err(e) = tp.link(&ghost) {
                        why.push(format!("link {tee_name}: {e:?}"));
                        ok = false;
                    }
                    pads.push((tee, tp));
                }
                None => {
                    why.push(format!("no pad on {tee_name}"));
                    ok = false;
                }
            }
        }
        if ok {
            if let Err(e) = bin.sync_state_with_parent() {
                why.push(format!("start: {e}"));
                ok = false;
            }
        }
        if !ok {
            crate::log(&format!("recording: hooking it in failed: {}", why.join("; ")));
        }
        if !ok {
            crate::log("recording couldn't be hooked into the picture");
            for (tee, tp) in pads {
                tee.release_request_pad(&tp);
            }
            let _ = bin.set_state(gst::State::Null);
            let _ = r.pipeline.remove(&bin);
            send(gen, Event::Recording(Err("Recording couldn't start.".into())));
            return;
        }
        *REC_AUDIO.lock().unwrap() = bin.by_name("raudio").and_then(|e| e.downcast::<gstreamer_app::AppSrc>().ok());
        crate::log(&format!("recording into {}", file.display()));
        r.rec = Some(Rec { bin, pads, file: file.clone(), since: Instant::now() });
        send(gen, Event::Recording(Ok(file)));
    }

    /// Stops recording: the file is finished (its end written) and closed; the picture goes on.
    pub fn record_stop() {
        let mut g = RUN.lock().unwrap();
        let Some(r) = g.as_mut() else { return };
        let Some(rec) = r.rec.take() else { return };
        let (p, gen) = (r.pipeline.clone(), r.gen);
        drop(g);
        finish_rec(&p, rec, gen, false);
    }

    /// Unhooks the recording from the tees, ends its stream (so mp4mux writes the file's end), waits for it to reach
    /// the file (at most 5 s), then takes it out of the pipeline.
    fn finish_rec(p: &gst::Pipeline, rec: Rec, gen: u64, blocking: bool) {
        // The sound's input ends first, so the file gets the end of both.
        if let Some(src) = REC_AUDIO.lock().unwrap().take() {
            let _ = src.end_of_stream();
        }
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        if let Some(fs) = rec.bin.by_name("rfile").and_then(|f| f.static_pad("sink")) {
            let tx = tx.clone();
            fs.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |_, info| {
                if let Some(gst::PadProbeData::Event(ev)) = &info.data {
                    if ev.type_() == gst::EventType::Eos {
                        let _ = tx.send(());
                    }
                }
                gst::PadProbeReturn::Ok
            });
        }
        for (_tee, tp) in &rec.pads {
            let Some(peer) = tp.peer() else { continue };
            tp.add_probe(gst::PadProbeType::IDLE, move |pad, _| {
                let _ = pad.unlink(&peer);
                let _ = peer.send_event(gst::event::Eos::new());
                gst::PadProbeReturn::Remove
            });
        }
        let p = p.clone();
        let file = rec.file.clone();
        let done = move || {
            let ok = rx.recv_timeout(Duration::from_secs(5)).is_ok();
            let _ = rec.bin.set_state(gst::State::Null);
            let _ = p.remove(&rec.bin);
            for (tee, tp) in &rec.pads {
                tee.release_request_pad(tp);
            }
            let good = ok && std::fs::metadata(&file).map(|m| m.len() > 0).unwrap_or(false);
            crate::log(&format!("recording ended (its end reached the file: {ok}; {} bytes)", std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0)));
            send(gen, Event::Recorded(if good { Ok(file.clone()) } else { Err(format!("The recording {} may be cut short.", file.file_name().unwrap_or_default().to_string_lossy())) }));
        };
        if blocking {
            done();
        } else {
            let _ = std::thread::Builder::new().name("cc-rec-end".into()).spawn(done);
        }
    }

    /// A recording's length, picture and a thumbnail (480 × 270) from its first second.
    pub fn probe(path: &Path) -> Option<Probe> {
        if !init() {
            return None;
        }
        let desc = format!(
            "filesrc location=\"{}\" ! decodebin ! videoconvert name=cv ! videoscale ! video/x-raw,format=RGBA,width=480,height=270,pixel-aspect-ratio=1/1 ! appsink name=t sync=false max-buffers=1",
            path.display().to_string().replace('"', "")
        );
        let p = gst::parse::launch(&desc).ok()?.downcast::<gst::Pipeline>().ok()?;
        let sink = p.by_name("t")?.downcast::<gstreamer_app::AppSink>().ok()?;
        let _ = p.set_state(gst::State::Paused);
        let (res, _, _) = p.state(gst::ClockTime::from_seconds(8));
        if res.is_err() {
            let _ = p.set_state(gst::State::Null);
            return None;
        }
        let secs = p.query_duration::<gst::ClockTime>().map(|d| d.seconds()).unwrap_or(0);
        if secs > 2 {
            let _ = p.seek_simple(gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT, gst::ClockTime::from_seconds(1));
            let _ = p.state(gst::ClockTime::from_seconds(5));
        }
        let sample = sink.try_pull_preroll(gst::ClockTime::from_seconds(5));
        let (mut w, mut h, mut fps) = (0, 0, 0);
        if let Some(c) = p.by_name("cv").and_then(|e| e.static_pad("sink")).and_then(|pd| pd.current_caps()) {
            if let Some(s) = c.structure(0) {
                w = s.get::<i32>("width").unwrap_or(0).max(0) as u32;
                h = s.get::<i32>("height").unwrap_or(0).max(0) as u32;
                if let Ok(f) = s.get::<gst::Fraction>("framerate") {
                    if f.denom() > 0 {
                        fps = (f.numer() as f64 / f.denom() as f64).round() as u32;
                    }
                }
            }
        }
        let rgba = sample.as_ref().and_then(|s| s.buffer().and_then(|b| b.map_readable().ok().map(|m| m.to_vec()))).unwrap_or_default();
        let _ = p.set_state(gst::State::Null);
        if rgba.len() < 480 * 270 * 4 {
            return None;
        }
        Some(Probe { secs, w, h, fps, tw: 480, th: 270, rgba: rgba[..480 * 270 * 4].to_vec() })
    }
}

pub use imp::{encoders, probe, record_start, record_stop, recording, running, restart_sound, set_volume, start, stop};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::card::Mode;

    fn ask() -> Ask {
        Ask {
            card: Card {
                dev: "/dev/video2".into(),
                id: String::new(),
                name: "Monster 4K".into(),
                chip: String::new(),
                speed: 5000,
                usb_port: String::new(),
                modes: vec![Mode { format: "MJPG".into(), w: 1920, h: 1080, fps: vec![60, 30] }, Mode { format: "MJPG".into(), w: 640, h: 480, fps: vec![30, 5] }],
                fake_picture: None,
                fake_sound: None,
            },
            format: "MJPG".into(),
            w: 1920,
            h: 1080,
            fps: 60,
            low_delay: true,
            sound: Some("alsa_input.usb-X".into()),
            play_sound: true,
            volume: 70,
            muted: false,
            watching: false,
        }
    }

    #[test]
    fn the_pipelines() {
        let d = describe(&ask(), true, true);
        assert!(d.starts_with("v4l2src name=src device=\"/dev/video2\" ! image/jpeg,width=1920,height=1080,framerate=60/1 ! vajpegdec"));
        assert!(d.contains("appsink name=pic sync=false max-buffers=1 drop=true"), "Low delay: no frame waits");
        assert!(d.contains("pipewiresrc name=asrc target-object=\"alsa_input.usb-X\""));
        assert!(d.contains("volume name=vol volume=0.70 mute=false ! pipewiresink"));
        let cpu = describe(&Ask { low_delay: false, play_sound: false, ..ask() }, false, true);
        assert!(cpu.contains("jpegdec name=dec") && cpu.contains("sync=true max-buffers=2"));
        assert!(!cpu.contains("pipewiresink"), "Play the Xbox's sound off: nothing plays");
        let y = describe(&Ask { format: "YUYV".into(), ..ask() }, false, false);
        assert!(y.contains("video/x-raw,format=YUY2") && y.contains("pulsesrc"));
        // Waiting for a signal: the lightest mode, no sound.
        let w = describe(&Ask { watching: true, ..ask() }, false, true);
        assert!(w.contains("width=640,height=480,framerate=5/1") && !w.contains("asrc"));
        // CI's stand-in card: its JPEG as the card's MJPEG, the rest the same.
        let mut a = ask();
        a.card.fake_picture = Some("/tmp/scene.jpg".into());
        let f = describe(&a, false, false);
        assert!(f.starts_with("appsrc name=src is-live=true do-timestamp=true format=time caps=\"image/jpeg,framerate=60/1\" ! jpegparse"));
        assert!(f.contains("audiotestsrc") && f.contains("jpegdec name=dec"));
        let r = describe_rec(Path::new("/v/Capture 1.mp4"), true, "vah264enc", Some("fdkaacenc"));
        assert!(r.contains("vapostproc ! video/x-raw,format=NV12") && r.contains("vah264enc bitrate=30000") && r.contains("mp4mux name=mux") && r.contains("fdkaacenc"));
        assert!(describe_rec(Path::new("/v/a.mp4"), false, "openh264enc", None).contains("openh264enc") );
        // The picture's pipeline never has the sound in it; the sound is its own (Nov 21, the Elgato 4K S).
        let v = describe_video(&ask(), true);
        assert!(!v.contains("asrc") && !v.contains("pipewiresink"));
        let snd = describe_sound(&ask(), true).unwrap();
        assert!(snd.starts_with("pipewiresrc name=asrc") && snd.contains("appsink name=recsink") && snd.contains("volume name=vol"));
        assert!(r.contains("appsrc name=raudio"), "a recording takes the sound from the sound's pipeline");
        // Through Pulse (what OBS uses) when it's there: in and out (Nov 21, no sound through pipewiresrc/sink).
        let ps = describe_sound(&ask(), false).unwrap();
        assert!(ps.starts_with("pulsesrc name=asrc device=") && ps.contains("pulsesink name=asink") && !ps.contains("pipewire"));
    }

    #[test]
    fn a_blank_picture_is_no_signal() {
        let (w, h) = (64u32, 36u32);
        let black = vec![0u8; (w * h * 4) as usize];
        assert!(blank(&black, w, h));
        let mut blue = black.clone();
        for p in blue.chunks_mut(4) {
            p[2] = 200;
        }
        assert!(blank(&blue, w, h), "a one-colour 'no signal' screen");
        let mut pic = black.clone();
        for (i, p) in pic.chunks_mut(4).enumerate() {
            p[0] = (i % 251) as u8;
            p[1] = (i * 7 % 253) as u8;
        }
        assert!(!blank(&pic, w, h));
        assert!(blank(&[], 0, 0));
    }
}
