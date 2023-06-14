if __name__ == "__main__":
    from .app import App
    app = App([])
    print("running...")
    raise SystemExit(app.run())
