"""
Generate .ico (Windows) and .icns (macOS) icon files from mp_logo.svg
using PySide6's QSvgRenderer — no external tools required.

Usage:
    python tools/generate_icons.py
"""
import struct
import sys
import zlib
from pathlib import Path

# Must create QApplication before any Qt rendering
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QImage, QPainter, QColor
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtCore import QRectF, Qt

HERE = Path(__file__).parent
ROOT = HERE.parent
SVG_PATH = ROOT / "resources" / "mp_logo.svg"
ICO_PATH = ROOT / "resources" / "mp_logo.ico"
ICNS_PATH = ROOT / "resources" / "mp_logo.icns"
PNG_PATH = ROOT / "resources" / "mp_logo_256.png"


def render_svg(renderer: QSvgRenderer, size: int) -> QImage:
    """Render the SVG at `size x size` onto a transparent QImage."""
    image = QImage(size, size, QImage.Format.Format_ARGB32)
    image.fill(QColor(0, 0, 0, 0))  # transparent background
    painter = QPainter(image)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
    renderer.render(painter, QRectF(0, 0, size, size))
    painter.end()
    return image


def image_to_rgba_bytes(image: QImage) -> bytes:
    """Convert QImage (ARGB32) to raw RGBA bytes."""
    # Qt stores ARGB32 as B,G,R,A in memory on little-endian — convert to RGBA
    image = image.convertToFormat(QImage.Format.Format_RGBA8888)
    ptr = image.bits()
    return bytes(ptr)


# ── ICO format writer ────────────────────────────────────────────────────────

ICO_SIZES = [16, 24, 32, 48, 64, 128, 256]


def build_ico(images: dict[int, QImage]) -> bytes:
    """
    Build a .ico file from a dict of {size: QImage}.
    Uses PNG compression for sizes > 48 (Vista+ format), raw BMP DIB for smaller.
    """
    entries = []
    for size in sorted(images):
        img = images[size]
        if size >= 64:
            # PNG-compressed entry (Windows Vista+)
            data = _image_to_png(img)
        else:
            # BMP DIB entry (BITMAPINFOHEADER + XOR mask + AND mask)
            data = _image_to_bmp_dib(img)
        entries.append((size, data))

    # ICO header: 6 bytes
    num = len(entries)
    header = struct.pack("<HHH", 0, 1, num)  # reserved=0, type=1 (ICO), count

    # Directory entries: 16 bytes each
    offset = 6 + num * 16
    dir_entries = b""
    image_data = b""
    for size, data in entries:
        w = size if size < 256 else 0
        h = size if size < 256 else 0
        dir_entries += struct.pack(
            "<BBBBHHII",
            w,          # width  (0 = 256)
            h,          # height (0 = 256)
            0,          # color count (0 = more than 256 or PNG)
            0,          # reserved
            1,          # color planes
            32,         # bits per pixel
            len(data),  # size of image data
            offset,     # offset of image data
        )
        image_data += data
        offset += len(data)

    return header + dir_entries + image_data


def _image_to_png(image: QImage) -> bytes:
    """Encode QImage as a PNG byte string."""
    from PySide6.QtCore import QBuffer, QByteArray, QIODevice
    buf = QByteArray()
    buffer = QBuffer(buf)
    buffer.open(QIODevice.OpenMode.WriteOnly)
    image.save(buffer, "PNG")
    buffer.close()
    return bytes(buf)


def _image_to_bmp_dib(image: QImage) -> bytes:
    """
    Encode QImage as a BMP DIB (no file header) suitable for ICO.
    Includes BITMAPINFOHEADER + pixel data (bottom-up) + AND mask.
    """
    size = image.width()
    image = image.convertToFormat(QImage.Format.Format_RGBA8888)

    # Build pixel rows bottom-up, converting RGBA → BGRA for Windows BMP
    pixel_data = bytearray()
    for row in range(size - 1, -1, -1):
        for col in range(size):
            c = image.pixel(col, row)
        # QImage.pixel returns ARGB as 0xAARRGGBB
    # Redo properly using scanLine
    pixel_data = bytearray()
    for row in range(size - 1, -1, -1):
        ptr = image.scanLine(row)
        # scanLine for Format_RGBA8888 gives R,G,B,A bytes
        row_bytes = bytes(ptr)[:size * 4]
        # Convert RGBA → BGRA
        for i in range(0, len(row_bytes), 4):
            r, g, b, a = row_bytes[i], row_bytes[i+1], row_bytes[i+2], row_bytes[i+3]
            pixel_data += bytes([b, g, r, a])

    # AND mask: 1-bit alpha mask, row-padded to DWORD boundary
    # For 32-bit ICO with alpha, the AND mask is all-zeros (transparent handled by alpha)
    mask_row_bytes = ((size + 31) // 32) * 4
    and_mask = b"\x00" * (mask_row_bytes * size)

    # BITMAPINFOHEADER (40 bytes), height is 2× for XOR+AND
    bih = struct.pack(
        "<IiiHHIIiiII",
        40,          # biSize
        size,        # biWidth
        size * 2,    # biHeight (×2 because ICO includes AND mask)
        1,           # biPlanes
        32,          # biBitCount
        0,           # biCompression (BI_RGB)
        0,           # biSizeImage
        0, 0,        # biX/YPelsPerMeter
        0, 0,        # biClrUsed, biClrImportant
    )

    return bih + bytes(pixel_data) + and_mask


# ── ICNS format writer ───────────────────────────────────────────────────────

# ICNS OSType → size mapping (we use PNG-compressed entries, 'ic' series)
ICNS_TYPES = [
    ("ic04", 16),
    ("ic05", 32),
    ("ic07", 128),
    ("ic08", 256),
    ("ic09", 512),
    ("ic10", 1024),
    ("ic11", 32),   # @2x Retina for 16pt
    ("ic12", 64),   # @2x Retina for 32pt
    ("ic13", 256),  # @2x Retina for 128pt
    ("ic14", 512),  # @2x Retina for 256pt
]

# Deduplicate by OSType, using the actual unique sizes we need
ICNS_SIZES_NEEDED = sorted({s for _, s in ICNS_TYPES})


def build_icns(images: dict[int, QImage]) -> bytes:
    """Build a .icns file from a dict of {size: QImage}."""
    chunks = b""
    for ostype, size in ICNS_TYPES:
        if size not in images:
            continue
        png_data = _image_to_png(images[size])
        ostype_bytes = ostype.encode("ascii")
        chunk_len = 8 + len(png_data)
        chunks += struct.pack(">4sI", ostype_bytes, chunk_len) + png_data

    total_len = 8 + len(chunks)
    header = struct.pack(">4sI", b"icns", total_len)
    return header + chunks


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    app = QApplication.instance() or QApplication(sys.argv)

    if not SVG_PATH.exists():
        print(f"ERROR: SVG not found: {SVG_PATH}")
        sys.exit(1)

    renderer = QSvgRenderer(str(SVG_PATH))
    if not renderer.isValid():
        print(f"ERROR: Could not load SVG: {SVG_PATH}")
        sys.exit(1)

    # Render all needed sizes
    all_sizes = sorted(set(ICO_SIZES) | set(ICNS_SIZES_NEEDED))
    print(f"Rendering SVG at sizes: {all_sizes}")
    images = {size: render_svg(renderer, size) for size in all_sizes}

    # Generate ICO
    ico_images = {s: images[s] for s in ICO_SIZES if s in images}
    ico_data = build_ico(ico_images)
    ICO_PATH.write_bytes(ico_data)
    print(f"Written: {ICO_PATH}  ({len(ico_data):,} bytes)")

    # Generate ICNS
    icns_images = {s: images[s] for s in ICNS_SIZES_NEEDED if s in images}
    icns_data = build_icns(icns_images)
    ICNS_PATH.write_bytes(icns_data)
    print(f"Written: {ICNS_PATH}  ({len(icns_data):,} bytes)")

    # Generate PNG (256×256) — used by the app at runtime on all platforms
    # and as the Linux executable icon source
    png_data = _image_to_png(images[256])
    PNG_PATH.write_bytes(png_data)
    print(f"Written: {PNG_PATH}  ({len(png_data):,} bytes)")

    print("Done.")


if __name__ == "__main__":
    main()
