from __future__ import annotations


def main() -> None:
    print("Tkinter desktop is retired for this migration stage.")
    print("Use the Tauri app flow instead:")
    print("  - Development: npm run tauri:dev  (from desktop_tauri/)")
    print("  - Production: install the built MSI from desktop_tauri/src-tauri/target/release/bundle/msi/")


if __name__ == "__main__":
    main()
