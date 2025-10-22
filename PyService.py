import dmPython
import pandas as pd

from flask import Flask, jsonify, request

import json
from typing import List, Dict

from datetime import datetime

# 创建Flask应用实例
app = Flask(__name__)

def get_multiple_result_sets():
    # 数据库连接参数
    conn_params = {
        'server': '192.168.0.191',  # 服务器地址
        'user': 'JZX',  # 用户名
        'password': 'XFgs@345',  # 密码
        'port': 5236,  # 端口号，默认5236
        'autoCommit': True  # 是否自动提交
    }

    result_sets = []  # 存储所有结果集
    try:
        # 建立连接
        conn = dmPython.connect(**conn_params)
        cursor = conn.cursor()

        # 调用返回多个结果集的存储过程
        cursor.callproc("JZX.GET_TEST0")

        # 获取所有结果集
        while True:
            # 获取列名
            columns = [column[0] for column in cursor.description] if cursor.description else []

            # 获取当前结果集的所有行
            rows = []
            for row in cursor.fetchall():
                # 将每行转换为字典
                row_dict = dict(zip(columns, row))
                rows.append(row_dict)

            # 如果有数据，添加到结果列表
            if rows:
                result_sets.append(rows)

            # 检查是否还有更多结果集
            if not cursor.nextset():
                break

        conn.commit()

    except Exception as e:
        print(f"操作错误: {str(e)}")
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

    return result_sets

def convert_datetime(obj):
    """将datetime对象转换为字符串，以便JSON序列化"""
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"Type {type(obj)} not serializable")

def tables_to_json(tables_data: Dict[str, List[Dict]]) -> str:
    """
    将多个表的数据转换为JSON字符串

    参数:
        tables_data: 字典，键为表名，值为表数据列表（每个元素是一行数据的字典）

    返回:
        包含所有表数据的JSON字符串
    """
    # 确保中文正常显示
    return json.dumps(tables_data, ensure_ascii=False, indent=2)

# 模拟数据库 - 存储用户数据
users = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]


# 1. 获取所有用户 (GET请求)
@app.route('/users', methods=['GET'])
def get_users():
    tables = get_multiple_result_sets()
    indent = 4
    # 转换为JSON
    json_result = json.dumps(
        tables,
        default=convert_datetime,
        ensure_ascii=False,
        indent=indent
    )

    return json_result


# 2. 获取单个用户 (GET请求，带参数)
@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    user = next((u for u in users if u['id'] == user_id), None)
    if user:
        return jsonify({"user": user})
    return jsonify({"error": "用户不存在"}), 404


# 启动服务
if __name__ == '__main__':
    # 允许外部访问，端口为5000，开启调试模式
    app.run(host='0.0.0.0', port=5000, debug=True)