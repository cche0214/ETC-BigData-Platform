from flask import Flask, jsonify
import happybase

# 初始化 Flask 应用
app = Flask(__name__)

# HBase Thrift 服务地址 (你 node1 的 IP + 8085 端口)
HBASE_THRIFT_HOST = "192.168.88.131"
HBASE_THRIFT_PORT = 8085

@app.route("/")
def home():
    """简单测试首页，验证后端是否正常运行"""
    return "🚦 ETC 大数据监测系统后端运行中"

@app.route("/api/hbase/traffic")
def get_hbase_traffic():
    """从 HBase 读取数据并返回 JSON"""
    try:
        # 1️⃣ 连接 HBase
        conn = happybase.Connection(HBASE_THRIFT_HOST, port=HBASE_THRIFT_PORT)
        table = conn.table('etc_traffic_data')

        # 2️⃣ 扫描表内容（可限制数量）
        result = []
        for key, data in table.scan(limit=100):  # limit=10 避免一次太多
            row = {'rowkey': key.decode('utf-8')}
            for k, v in data.items():
                cf, col = k.decode('utf-8').split(':')
                row[col] = v.decode('utf-8')
            result.append(row)

        conn.close()

        # 3️⃣ 返回 JSON 响应
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        # 错误时返回
        return jsonify({"status": "error", "msg": str(e)})

if __name__ == "__main__":
    # Flask 默认监听 8080 端口
    app.run(host="0.0.0.0", port=8080, debug=True)
