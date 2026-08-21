# housingmetrics.nyc logo

Two overlapping bars on a warm ground: a tall bar and a short bar that cross,
darkening where they meet. It reads two ways on purpose — as a bar chart
(metrics) and as a pair of buildings (housing) — and the intersection stands
for the thing this project actually does: joining separate housing datasets
into one picture.

## Files

| File | Use |
| --- | --- |
| `housingmetrics-mark.svg` | Primary mark (app icon, avatar, social profile) |
| `housingmetrics-mark-dark.svg` | Primary mark on dark backgrounds |
| `housingmetrics-mark-mono.svg` | Single-color mark; ships transparent and inherits `currentColor` |
| `housingmetrics-logo-horizontal.svg` | Mark + wordmark, for headers and README banners |
| `housingmetrics-logo-stacked.svg` | Centered mark + wordmark + tagline, for title cards and slides |
| `housingmetrics-favicon.svg` | Tighter crop that stays legible at 16px |
| `png/` | Rasterized exports (mark 512/192, favicon 32/64, apple-touch 180, `favicon.ico`, horizontal lockup) |

Open `preview.html` in a browser to see every variant at several sizes.

## Palette

| Role | Hex |
| --- | --- |
| Ground (warm off-white) | `#E8E3DD` |
| Bar, light | `#A3B2C8` |
| Bar, mid | `#78879E` |
| Intersection, dark | `#58657A` |
| Wordmark ink | `#2E3746` |
| Dark ground | `#1C222C` |

## Usage

- Keep clear space around the mark equal to the width of the short bar (a
  quarter of the mark's width).
- Don't recolor, rotate, outline, or add effects to the bars; use
  `housingmetrics-mark-mono.svg` when a single color is required. It resolves
  `currentColor`, so inline it (or use it as a CSS mask) — referenced through an
  `<img>` tag it renders black.
- Below 24px use the favicon crop rather than shrinking the primary mark.
- The lockups use live text in `Inter`, falling back to Helvetica/Arial. For
  print or anywhere the font stack isn't guaranteed, export a PNG (or convert
  the text to outlines) first.

## Regenerating the PNGs

The exports were rendered from the SVGs with headless Chromium and downsampled
with Pillow; any SVG rasterizer works, e.g.:

```sh
rsvg-convert -w 512 -h 512 housingmetrics-mark.svg -o png/housingmetrics-mark-512.png
```
