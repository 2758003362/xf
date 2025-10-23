import dmPython
import pandas as pd
import os
from flask import Flask, jsonify, Response, request
import json
from typing import List, Dict
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import xml.etree.ElementTree as ET

app = Flask(__name__)


def convert_datetime(obj):
    """将datetime对象转换为JSON可序列化字符串"""
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"类型 {type(obj)} 不支持JSON序列化")


def get_multiple_result_sets(strSp, strParam):
    """调用达梦存储过程（增强版：细化连接错误信息）"""
    # 加密库检查（保留核心调试）
    print("\n" + "=" * 50)
    print("【加密库加载调试】- 开始检查依赖文件")
    print(f"1. 当前运行目录：{os.getcwd()}")
    print(f"2. LD_LIBRARY_PATH：{os.environ.get('LD_LIBRARY_PATH', '未设置')}")

    required_dm_libs = [
        "libcryptocme.so", "libdmcrypt.so", "libdmdpi.so", "libdmgmssl.so"
    ]
    missing_libs = []
    for lib in required_dm_libs:
        lib_path = os.path.join(os.getcwd(), lib)
        if os.path.exists(lib_path) and os.path.getsize(lib_path) > 0:
            print(f"✅ 找到有效加密库：{lib_path}（大小：{os.path.getsize(lib_path)}字节）")
        else:
            print(f"❌ 缺失或损坏的加密库：{lib_path}")
            missing_libs.append(lib)

    if missing_libs:
        error_msg = f"【致命错误】缺失加密库：{', '.join(missing_libs)}，无法连接数据库"
        print(error_msg)
        raise Exception(error_msg)

    # 数据库连接参数（核心配置）
    conn_params = {
        'server': '192.168.0.191',  # 数据库地址（必填）
        'user': 'JZX',  # 用户名（必填）
        'password': 'XFgs@345',  # 密码（必填）
        'port': 5236,  # 端口（默认5236）
        'autoCommit': True
    }
    result_sets = []
    conn = None
    cursor = None

    try:
        print("\n【数据库操作】开始连接达梦数据库...")
        print(f"连接参数：server={conn_params['server']}, port={conn_params['port']}, user={conn_params['user']}")

        # ------------ 核心增强：细化连接阶段错误捕获 ------------
        try:
            # 尝试建立连接（此步骤最易出错，单独捕获）
            conn = dmPython.connect(**conn_params)
        except dmPython.DatabaseError as e:
            # 达梦数据库返回的具体错误（含错误码和描述）
            error_detail = (
                f"达梦连接失败 [错误码: {e.errno}]\n"
                f"错误描述: {e.strerror}\n"
                f"可能原因: \n"
                f"  1. 数据库地址/端口错误（当前：{conn_params['server']}:{conn_params['port']}\n"
                f"  2. 用户名/密码错误（当前用户：{conn_params['user']}\n"
                f"  3. 数据库服务未启动或端口未开放\n"
                f"  4. 加密库版本与数据库不兼容"
            )
            print(f"❌ {error_detail}")
            raise Exception(error_detail) from e
        # ------------------------------------------------------

        print("✅ 数据库连接成功（加密模块加载正常）")
        cursor = conn.cursor()

        # 检查存储过程名是否为空
        if not strSp:
            raise Exception("【参数错误】存储过程名（param1）不能为空")

        # 调用存储过程（确保参数为元组格式）
        print(f"调用存储过程：{strSp}，参数：{strParam}")
        try:
            cursor.callproc(strSp, (strParam,))  # 注意参数末尾的逗号
        except dmPython.DatabaseError as e:
            error_detail = (
                f"存储过程调用失败 [错误码: {e.errno}]\n"
                f"错误描述: {e.strerror}\n"
                f"可能原因: \n"
                f"  1. 存储过程 {strSp} 不存在\n"
                f"  2. 参数 {strParam} 格式错误或不合法\n"
                f"  3. 存储过程内部执行出错"
            )
            print(f"❌ {error_detail}")
            raise Exception(error_detail) from e

        # 获取结果集
        set_index = 1
        while True:
            if cursor.description:  # 存在结果集
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                result_sets.append(rows)
                print(f"✅ 获取结果集 {set_index}：{len(rows)} 行数据")
                set_index += 1
            if not cursor.nextset():  # 无更多结果集
                break

        conn.commit()
        print(f"✅ 所有结果集处理完成（共 {len(result_sets)} 个）")
        return result_sets

    except Exception as e:
        # 统一处理所有异常，确保资源释放
        print(f"❌ 处理中断：{str(e)}")
        if conn:
            try:
                conn.rollback()
                print("ℹ️  事务已回滚")
            except:
                pass
        raise  # 向上层传递错误，便于接口返回

    finally:
        # 确保游标和连接关闭
        if cursor:
            try:
                cursor.close()
                print("ℹ️  游标已关闭")
            except:
                pass
        if conn:
            try:
                conn.close()
                print("ℹ️  数据库连接已关闭")
            except:
                pass


# 接口定义（保留完整功能，错误返回更详细）
@app.route('/users', methods=['GET', 'POST'])
def get_users():
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1', '')
            param2 = request.args.get('param2', '')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        print(f"\n【接口请求】/users - param1: {param1}, param2: {param2}")
        result_data = get_multiple_result_sets(param1, param2)
        return json.dumps(
            result_data,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        ), 200, {'Content-Type': 'application/json; charset=utf-8'}

    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),  # 包含详细错误原因
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'param1': param1,  # 附带请求参数，便于排查
            'param2': param2
        }), 500


@app.route('/jsonService', methods=['GET', 'POST'])
def get_json():
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1', '')
            param2 = request.args.get('param2', '')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        print(f"\n【接口请求】/jsonService - param1: {param1}, param2: {param2}")
        result_data = get_multiple_result_sets(param1, param2)
        return json.dumps(
            result_data,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        ), 200, {'Content-Type': 'application/json; charset=utf-8'}

    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'param1': param1,
            'param2': param2
        }), 500


@app.route('/xmlService', methods=['GET', 'POST'])
def get_xml(encoding="utf-8"):
    try:
        if request.method == 'GET':
            param1 = request.args.get('param1', '')
            param2 = request.args.get('param2', '')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        print(f"\n【接口请求】/xmlService - param1: {param1}, param2: {param2}")
        result_data = get_multiple_result_sets(param1, param2)

        # 生成XML响应
        root = ET.Element("ResultSets")
        root.set("generated_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        root.set("total_sets", str(len(result_data)))

        for set_idx, result_set in enumerate(result_data, 1):
            if not result_set:
                continue
            columns = list(result_set[0].keys()) if result_set else []
            set_node = ET.SubElement(root, "ResultSet")
            set_node.set("id", str(set_idx))
            set_node.set("row_count", str(len(result_set)))
            set_node.set("column_count", str(len(columns)))

            cols_node = ET.SubElement(set_node, "Columns")
            for col in columns:
                ET.SubElement(cols_node, "Column").text = col

            rows_node = ET.SubElement(set_node, "Rows")
            for row_idx, row in enumerate(result_set, 1):
                row_node = ET.SubElement(rows_node, "Row")
                row_node.set("index", str(row_idx))
                for col_idx, col_name in enumerate(columns):
                    cell_value = row[col_name]
                    cell_text = cell_value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(cell_value, datetime) else (
                        str(cell_value) if cell_value is not None else "")
                    cell_node = ET.SubElement(row_node, "Cell")
                    cell_node.set("column", col_name)
                    cell_node.set("column_index", str(col_idx))
                    cell_node.text = cell_text

        rough_xml = ET.tostring(root, encoding=encoding)
        pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent="  ", encoding=encoding)
        clean_xml = "\n".join([line for line in pretty_xml.decode(encoding).split("\n") if line.strip()])
        return Response(clean_xml, mimetype=f'application/xml; charset={encoding}')

    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'param1': param1,
            'param2': param2
        }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("【达梦API服务】启动中...")
    print(f"启动时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"运行目录：{os.getcwd()}")
    print("服务地址：http://0.0.0.0:5000")
    print("可用接口：/users, /jsonService, /xmlService（支持GET/POST）")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5000, debug=True)