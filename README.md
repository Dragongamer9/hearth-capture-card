# Capture Card

A Hearth Store app: your Xbox (or anything with HDMI) on this PC's screen through a USB capture card, full screen, with
its sound, screenshots and recordings. Made for Hearth OS (Fedora edition) by Dragongamer9. Version 1.0.

- Install it from the **Hearth Store** on Hearth OS, or download the AppImage from this repository's
  [releases](https://github.com/Dragongamer9/hearth-capture-card/releases) and run it.
- It uses Hearth OS's own GStreamer for the picture and sound (UVC cards: MJPEG or YUYV, 1080p60; the card's sound
  as a USB microphone), with the graphics card's decoder where there is one.
- Works with the mouse and keyboard, a touchpad or a controller, on the desktop and in Game Mode.

The approved mockups and their checklist live in Hearth OS (`docs/mockups/capture-card`,
`docs/mockups/checklists/capture-card.md`).

## Building

Built and tested only in GitHub Actions (`.github/workflows/ci.yml`): the build and tests in Fedora 45, a stand-in
card for every page, the AppImage. It uses Hearth's shared look and widgets (`hearth-ui`) from Hearth OS at the commit
in `hearth-os.ref` (checked out into `hearth-os/`). A release is made by hand (Actions › Capture Card › Run workflow,
"release" ticked) with the version in `Cargo.toml`; the owner picks every version number.

License: GPL-3.0-or-later.
