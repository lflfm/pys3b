"""Module entry point for the S3 browser application."""
import argparse
import logging
import sys

from PySide6 import QtWidgets

from .qt_view import S3BrowserWindow


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 Object Browser")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args, remaining = parser.parse_known_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

    app = QtWidgets.QApplication([sys.argv[0], *remaining])
    window = S3BrowserWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
