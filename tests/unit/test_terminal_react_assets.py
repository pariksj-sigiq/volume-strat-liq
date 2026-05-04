from pathlib import Path

import app.server as server


def test_terminal_route_prefers_built_react_app_when_available(tmp_path, monkeypatch) -> None:
    dist = tmp_path / "web" / "dist"
    assets = dist / "assets"
    assets.mkdir(parents=True)
    index = dist / "index.html"
    bundle = assets / "terminal.js"
    index.write_text("<div id='root'></div>", encoding="utf-8")
    bundle.write_text("console.log('terminal')", encoding="utf-8")

    monkeypatch.setattr(server, "WEB_DIST_DIR", dist)

    assert server._resolve_static_path("/terminal") == index
    assert server._resolve_static_path("/assets/terminal.js") == bundle


def test_legacy_assets_still_resolve_when_react_build_does_not_own_path(tmp_path, monkeypatch) -> None:
    dist = tmp_path / "web" / "dist"
    (dist / "assets").mkdir(parents=True)
    monkeypatch.setattr(server, "WEB_DIST_DIR", dist)

    assert server._resolve_static_path("/assets/styles.css") == server.STATIC_DIR / "styles.css"
