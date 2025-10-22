import dmPython
import pandas as pd
import os  # 新增：用于获取环境变量和检查文件
import glob  # 新增：用于查找库文件

from flask import Flask, jsonify, Response, request

import json
from typing import List, Dict

from datetime import datetime

from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import xml.etree.ElementTree as ET

app = Flask(__name__)


def convert_datetime(obj):
    """将datetime对象转换为字符串，以便JSON序列化"""
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"Type {type(obj)} not serializable")


def get_multiple_result_sets(strSp, strParam):
    # 新增：调试信息 - 检查环境变量和库文件
    print("\n===== 加密库加载调试信息 =====")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH', '未设置')}")

    # 检查当前目录下的达梦加密库
    required_libs = [
        "libcryptocme.so",
        "libdmcrypt.so",
        "libdmdpi.so",
        "libdmgmssl.so"
    ]
    for lib in required_libs:
        lib_path = os.path.join(os.getcwd(), lib)
        if os.path.exists(lib_path):
            print(f"找到加密库: {lib_path} (大小: {os.path.getsize(lib_path)} bytes)")
        else:
            print(f"警告：未找到加密库 {lib_path}")

    # 数据库连接参数
    conn_params = {
        'server': 'localhost',  # 服务器地址
        'user': 'JZX',  # 用户名
        'password': 'XFgs@345',  # 密码
        'port': 5236,  # 端口号，默认5236
        'autoCommit': True  # 是否自动提交
    }

    result_sets = []
    conn = None  # 初始化conn为None
    cursor = None  # 初始化cursor为None
    try:
        print("\n尝试连接数据库...")
        # 建立连接（此处是加密模块加载的关键步骤）
        conn = dmPython.connect(**conn_params)
        print("数据库连接成功！")

        cursor = conn.cursor()
        cursor.callproc(strSp, (strParam,))  # 确保参数为元组格式

        # 获取所有结果集
        while True:
            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            if rows:
                result_sets.append(rows)
            if not cursor.nextset():
                break

        conn.commit()
        print(f"成功获取 {len(result_sets)} 个结果集")

    except dmPython.DatabaseError as e:
        # 达梦数据库特定错误（包含加密模块错误）
        print(f"达梦数据库错误: [CODE:{e.errno}] {e.strerror}")
        if conn is not None:
            try:
                conn.rollback()
                print("事务已回滚")
            except Exception as rollback_err:
                print(f"回滚失败: {str(rollback_err)}")
    except Exception as e:
        # 其他通用错误
        print(f"操作错误: {str(e)}")
        if conn is not None:
            try:
                conn.rollback()
            except:
                pass
    finally:
        # 确保资源关闭
        if cursor is not None:
            try:
                cursor.close()
                print("游标已关闭")
            except:
                pass
        if conn is not None:
            try:
                conn.close()
                print("数据库连接已关闭")
            except:
                pass

    return result_sets


def tables_to_json(tables_data: Dict[str, List[Dict]]) -> str:
    return json.dumps(tables_data, ensure_ascii=False, indent=2)


@app.route('/users', methods=['GET', 'POST'])
def get_users():
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1')
            param2 = request.args.get('param2')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1')
            param2 = data.get('param2')

        tables = get_multiple_result_sets(param1, param2)
        return json.dumps(
            tables,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'处理错误: {str(e)}'
        }), 500


@app.route('/jsonService', methods=['GET', 'POST'])
def get_json():
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1')
            param2 = request.args.get('param2')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        tables = get_multiple_result_sets(param1, param2)
        return json.dumps(
            tables,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'处理错误: {str(e)}'
        }), 500


def result_sets_to_xml(result_sets, root_name="ResultSets", encoding="utf-8"):
    root = ET.Element(root_name)
    root.set("generated_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    root.set("total_sets", str(len(result_sets)))

    for set_idx, result_set in enumerate(result_sets, 1):
        if not result_set:
            continue
        columns = list(result_set[0].keys()) if result_set else []
        table_name = f"ResultSet_{set_idx}"

        set_node = ET.SubElement(root, "ResultSet")
        set_node.set("id", str(set_idx))
        set_node.set("table_name", table_name)
        set_node.set("row_count", str(len(result_set)))
        set_node.set("column_count", str(len(columns)))

        columns_node = ET.SubElement(set_node, "Columns")
        for col in columns:
            SubElement(columns_node, "Column").text = col

        rows_node = ET.SubElement(set_node, "Rows")
        for row_idx, row in enumerate(result_set, 1):
            row_node = ET.SubElement(rows_node, "Row")
            row_node.set("index", str(row_idx))
            for col_idx, col_name in enumerate(columns):
                value = row.get(col_name)
                if isinstance(value, datetime):
                    cell_text = value.strftime("%Y-%m-%d %H:%M:%S")
                elif value is None:
                    cell_text = ""
                else:
                    cell_text = str(value)
                cell_node = ET.SubElement(row_node, "Cell")
                cell_node.set("column", col_name)
                cell_node.set("column_index", str(col_idx))
                cell_node.text = cell_text

    rough_xml = ET.tostring(root, encoding=encoding)
    pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent="  ", encoding=encoding)
    return "\n".join([line for line in pretty_xml.decode(encoding).split("\n") if line.strip()])


@app.route('/xmlService', methods=['GET', 'POST'])
def get_xml(encoding="utf-8"):
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1')
            param2 = request.args.get('param2')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        tables = get_multiple_result_sets(param1, param2)
        return Response(result_sets_to_xml(tables), mimetype='application/xml')
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'处理错误: {str(e)}'
        }), 500


if __name__ == '__main__':
    # 启动时打印额外调试信息
    print("===== 服务启动信息 =====")
    print(f"Python 环境: {os.environ.get('PYTHONPATH', '未设置')}")
    print(f"当前目录: {os.getcwd()}")
    app.run(host='0.0.0.0', port=5000, debug=True)