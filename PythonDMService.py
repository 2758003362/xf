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

def get_multiple_result_sets(strSp,strParam):
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
        # strSp "JZX.GET_TEST0"
        cursor.callproc(strSp,(strParam))

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

# 1. 获取所有用户 (GET请求)
@app.route('/users', methods=['GET','POST'])
def get_users():
    if request.method == 'GET':
        # 处理GET请求，从查询参数获取
        param1 = request.args.get('param1') #request.args.get('param1')
        param2 = request.args.get('param2')
    else:
        # 处理POST请求，先尝试从JSON获取，再尝试从表单获取
        data = request.get_json() or request.form
        param1 = data.get('param1') if data.get('param1') else None
        param2 = data.get('param2') if data.get('param2') else None

    #return jsonify({"param1": param1,"param2": param2})
    tables = get_multiple_result_sets(param1,param2)

    # 转换为JSON
    json_result = json.dumps(
        tables,
        default=convert_datetime,
        ensure_ascii=False,
        indent=4
    )

    return json_result

# 1.返回存储过程数据JSON格式 (GET POST请求)
@app.route('/jsonService', methods=['GET','POST'])
def get_json():
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

        # return jsonify({"param1": param1, "param2": param2})
        tables = get_multiple_result_sets(param1, param2)

        # 转换为JSON
        json_result = json.dumps(
            tables,
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

def result_sets_to_xml(result_sets, root_name="ResultSets", encoding="utf-8"):
    """
    将result_sets列表（包含多个结果集）转换为XML
    :param result_sets: 结果集列表，每个元素为 (表名, 列名列表, 数据行列表)
    :param root_name: XML根节点名称
    :param encoding: 编码格式
    :return: 格式化的XML字符串
    """
    # 创建根节点
    root = ET.Element(root_name)
    root.set("generated_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    root.set("total_sets", str(len(result_sets)))  # 总结果集数量

    # 遍历result_sets中的每个结果集
    for set_idx, result_set in enumerate(result_sets, 1):
        # 假设每个结果集的结构为 (表名, 列名列表, 数据行列表)
        table_name, columns, rows = result_set

        # 创建结果集节点
        set_node = ET.SubElement(root, "ResultSet")
        set_node.set("id", str(set_idx))  # 结果集序号（从1开始）
        set_node.set("table_name", table_name)  # 表名
        set_node.set("row_count", str(len(rows)))  # 行数
        set_node.set("column_count", str(len(columns)))  # 列数

        # 添加列定义节点
        columns_node = ET.SubElement(set_node, "Columns")
        for col in columns:
            col_node = ET.SubElement(columns_node, "Column")
            col_node.text = col  # 列名

        # 添加数据行节点
        rows_node = ET.SubElement(set_node, "Rows")
        for row_idx, row in enumerate(rows, 1):
            row_node = ET.SubElement(rows_node, "Row")
            row_node.set("index", str(row_idx))  # 行序号

            # 处理每行数据（与列对应）
            for col_idx, value in enumerate(row):
                # 处理特殊数据类型
                if isinstance(value, datetime):
                    # 日期时间格式化
                    cell_text = value.strftime("%Y-%m-%d %H:%M:%S")
                elif value is None:
                    # NULL值处理为空字符串
                    cell_text = ""
                else:
                    # 其他类型转为字符串
                    cell_text = str(value)

                # 创建单元格节点，关联列信息
                cell_node = ET.SubElement(row_node, "Cell")
                cell_node.set("column", columns[col_idx] if col_idx < len(columns) else f"col_{col_idx}")
                cell_node.set("column_index", str(col_idx))
                cell_node.text = cell_text

    # 格式化XML（增加缩进）
    rough_xml = ET.tostring(root, encoding=encoding)
    pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent="  ", encoding=encoding)

    # 去除空行，返回整洁的XML字符串
    return "\n".join([line for line in pretty_xml.decode(encoding).split("\n") if line.strip()])

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

        # return jsonify({"param1": param1, "param2": param2})
        tables = get_multiple_result_sets(param1, param2)

        xml_data = result_sets_to_xml(tables)
        return xml_data
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'处理错误: {str(e)}'
        })
# 启动服务
if __name__ == '__main__':
    # 允许外部访问，端口为5000，开启调试模式
    app.run(host='0.0.0.0', port=5000, debug=True)