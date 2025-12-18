"""Module entry point for the S3 browser application."""
import tkinter as tk

from .tk_view import S3BrowserApp


def main() -> None:
    root = tk.Tk()
    S3BrowserApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
