"""Flask Web 层：五端口虚拟交换机实验台（积压帧磁盘暂存版）。

直接运行:  python app.py        （监听 0.0.0.0:8000，threaded=True）
容器运行:  见 Dockerfile / docker-compose.yml
环境变量:
  SPOOL_DIR          积压帧暂存目录（默认 /tmp/vlan-switch-spool）
  ENABLE_TEST_HOOKS  置 1 时开放 /api/test/* 故障注入钩子（仅自测用）
"""

import os

from flask import Flask, jsonify, render_template, request

from switch import SpoolWriteError, Switch

# 暂存目录可由环境变量覆盖；模块导入时（含 gunicorn 等场景）即固定。
SPOOL_DIR = os.environ.get("SPOOL_DIR", "/tmp/vlan-switch-spool")
ENABLE_TEST_HOOKS = os.environ.get("ENABLE_TEST_HOOKS", "") == "1"


def create_app(switch=None):
    app = Flask(__name__)
    sw = switch if switch is not None else Switch(spool_dir=SPOOL_DIR)

    def body():
        return request.get_json(silent=True) or {}

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/state")
    def api_state():
        # 默认只回队尾 10 帧（自动刷新内存有界）；?full=1 显式拉完整积压明细
        full = request.args.get("full", "") in ("1", "true", "yes")
        return jsonify(sw.state(include_full_queue=full))

    @app.post("/api/frame")
    def api_frame():
        data = body()
        try:
            return jsonify(sw.send_frame(
                data.get("port"), data.get("src", ""),
                data.get("dst", ""), data.get("payload", "")))
        except (ValueError, SpoolWriteError) as e:
            # ValueError：输入非法；SpoolWriteError：暂存写盘失败、整帧作废
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/vlan")
    def api_set_vlan(port_id):
        try:
            return jsonify(sw.set_vlan(port_id, body().get("vlan")))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/pause")
    def api_pause(port_id):
        try:
            return jsonify(sw.pause(port_id))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/release")
    def api_release(port_id):
        try:
            return jsonify(sw.release(port_id))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/reset")
    def api_reset():
        return jsonify(sw.reset())

    # ---------- 自测钩子（默认关闭，容器内通过 ENABLE_TEST_HOOKS=1 打开） ----------
    if ENABLE_TEST_HOOKS:
        @app.post("/api/test/fail-writes")
        def api_test_fail_writes():
            return jsonify(sw.set_fail_writes(bool(body().get("enabled", True))))

    return app


app = create_app()

if __name__ == "__main__":
    # threaded=True：同一端口的改配与放行等请求可并发到达，
    # 由 Switch 内部的锁保证各自原子生效。
    app.run(host="0.0.0.0", port=8000, threaded=True)
