import dmPython
import pandas as pd

from flask import Flask, jsonify, Response,request

import json
from typing import List, Dict

from datetime import datetime

from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import xml.etree.ElementTree as ET

# 创建Flask应用实例
app = Flask(__name__)

def convert_datetime(obj):
    """将datetime对象转换为字符串，以便JSON序列化"""
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"Type {type(obj)} not serializable")

def get_cursor(strSp,strParam):
    # 数据库连接参数
    conn_params = {
        'server': '192.168.0.191',  # 服务器地址
        'user': 'JZX',  # 用户名
        'password': 'XFgs@345',  # 密码
        'port': 5236,  # 端口号，默认5236
        'autoCommit': True  # 是否自动提交
    }

    try:
        # 建立连接
        conn = dmPython.connect(**conn_params)
        cursor = conn.cursor()

        # 调用返回多个结果集的存储过程
        # strSp "JZX.GET_TEST0"
        cursor.callproc(strSp,(strParam))

        #conn.commit()

        # 4. 提取所有隐式结果集（通过nextset()切换游标）
        root = ET.Element("MultiResultSet")  # 根节点
        table_index = 1  # 表序号

        while True:
            # 获取当前结果集的列名
            try:
                columns = [desc[0] for desc in cursor.description]
            except Exception:
                # 无更多结果集时，description会报错
                break

            # 获取当前结果集的所有行数据
            rows = cursor.fetchall()

            # 封装当前结果集为XML子节点
            table_node = ET.SubElement(root, f"Table{table_index}")

            # 添加字段信息
            cols_node = ET.SubElement(table_node, "Columns")
            for col in columns:
                ET.SubElement(cols_node, "Column").text = col

            # 添加行数据
            rows_node = ET.SubElement(table_node, "Rows")
            for row in rows:
                row_node = ET.SubElement(rows_node, "Row")
                for idx, value in enumerate(row):
                    ET.SubElement(row_node, columns[idx]).text = str(value) if value is not None else ""

            # 切换到下一个结果集（游标）
            if not cursor.nextset():
                break  # 无更多结果集时退出循环

            table_index += 1

        # 格式化XML
        rough_xml = ET.tostring(root, 'utf-8')
        parsed_xml = minidom.parseString(rough_xml)
        formatted_xml = parsed_xml.toprettyxml(indent="  ")

        return formatted_xml

    except Exception as e:
        print(f"执行错误：{str(e)}")
        return f"<Error>{str(e)}</Error>"
    finally:
        if conn:
            conn.close()

# 1.返回存储过程数据JSON格式 (GET POST请求)
@app.route('/xmlService', methods=['GET', 'POST'])
def get_xml(encoding = "utf-8"):
    try:
        if request.method == 'GET':
            # 处理GET请求，从查询参数获取
            param1 = request.args.get('param1')  # request.args.get('param1')
            param2 = request.args.get('param2')
        else:
            # 获取JSON数据
            json_str = request.data or request.get_json() or request.form

            # return json_str
            data = json.loads(json_str)

            # 提取param1和param2
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        # cursor = get_cursor(param1, param2)

        # return "<ROOT><param1>" + param1 + "</param1><param2>" + param2 + "</param2></ROOT>"

        strXml = get_cursor(param1, param2)
        return strXml

        # 2. 将游标转换为result_sets列表
        result_sets = cursor_to_result_sets(cursor)

        # 转换为JSON
        json_result = json.dumps(
            result_sets,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        )

        return json_result

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'处理错误: {str(e)}'
        })
# 启动服务
if __name__ == '__main__':
    # 允许外部访问，端口为5000，开启调试模式
    app.run(host='0.0.0.0', port=5000, debug=True)