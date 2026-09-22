def test_all_modules_importable() -> None:
    import gateway.app
    import gateway.auth
    import gateway.commands
    import gateway.devices
    import gateway.errors
    import gateway.models
    import gateway.telemetry
    import gateway.versions

    assert gateway.app.create_app is not None
