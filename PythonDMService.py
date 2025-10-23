import dmPython
import pandas as pd
import os  # 用于环境变量和文件检查
from flask import Flask, jsonify, Response, request
import json
from typing import List, Dict
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import xml.etree.ElementTree as ET

# 创建Flask应用实例
app = Flask(__name__)


def convert_datetime(obj):
    """将datetime对象转换为JSON可序列化的字符串"""
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"类型 {type(obj)} 不支持JSON序列化")


def get_multiple_result_sets(strSp, strParam):
    """调用达梦存储过程，获取多结果集（含加密库加载调试）"""
    # -------------------------- 核心调试：加密库检查 --------------------------
    print("\n" + "=" * 50)
    print("【加密库加载调试】- 开始检查依赖文件")
    print(f"1. 当前运行目录：{os.getcwd()}")
    print(f"2. LD_LIBRARY_PATH：{os.environ.get('LD_LIBRARY_PATH', '未设置（可能导致库加载失败）')}")

    # 定义必须的达梦加密库列表（缺一不可）
    required_dm_libs = [
        "libcryptocme.so",
        "libdmcrypt.so",
        "libdmdpi.so",
        "libdmgmssl.so"
    ]
    missing_libs = []
    for lib in required_dm_libs:
        lib_full_path = os.path.join(os.getcwd(), lib)
        if os.path.exists(lib_full_path):
            # 检查文件是否为空（避免损坏）
            if os.path.getsize(lib_full_path) > 0:
                print(f"✅ 找到有效加密库：{lib_full_path}（大小：{os.path.getsize(lib_full_path)} 字节）")
            else:
                print(f"❌ 加密库 {lib_full_path} 为空（文件损坏）")
                missing_libs.append(lib)
        else:
            print(f"❌ 未找到加密库：{lib_full_path}")
            missing_libs.append(lib)

    # 若存在缺失库，直接提示解决方案
    if missing_libs:
        print(f"\n【关键错误】缺失以下加密库：{', '.join(missing_libs)}")
        print("解决方案：")
        print("1. 从达梦数据库安装目录（如 /opt/dmdbms/bin）复制这些库文件到当前目录")
        print("2. 确认GitHub仓库的 dm_libs/ 目录包含这些文件，重新打包")
        print("=" * 50 + "\n")

    # -------------------------- 数据库连接逻辑 --------------------------
    conn_params = {
        'server': 'localhost',
        'user': 'JZX',
        'password': 'XFgs@345',
        'port': 5236,
        'autoCommit': True
    }
    result_sets = []
    conn = None  # 初始化连接对象，避免未定义错误
    cursor = None  # 初始化游标对象

    try:
        print("\n【数据库操作】尝试建立达梦连接...")
        # 核心：达梦连接（加密模块在此步骤加载）
        conn = dmPython.connect(**conn_params)
        print("✅ 达梦数据库连接成功（加密模块加载正常）")

        cursor = conn.cursor()
        # 调用存储过程（参数必须是元组，末尾逗号不可少）
        cursor.callproc(strSp, (strParam,))

        # 循环获取所有结果集
        set_index = 1
        while True:
            # 获取列名（无结果集时cursor.description为None）
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                # 转换行数据为字典（便于JSON序列化）
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                if rows:
                    result_sets.append(rows)
                    print(f"✅ 获取结果集 {set_index}：{len(rows)} 行数据")
                    set_index += 1
            # 检查是否有下一个结果集（无则退出循环）
            if not cursor.nextset():
                break

        conn.commit()
        print(f"✅ 所有结果集处理完成（共 {len(result_sets)} 个）")

    except dmPython.DatabaseError as e:
        # 达梦特定错误（含加密模块错误）
        error_msg = f"【达梦错误】[错误码: {e.errno}] {e.strerror}"
        print(f"❌ {error_msg}")
        # 仅当连接已创建时尝试回滚
        if conn:
            try:
                conn.rollback()
                print("ℹ️  事务已回滚")
            except Exception as rollback_err:
                print(f"❌ 回滚失败：{str(rollback_err)}")
        # 向上层传递错误信息（便于接口返回）
        raise Exception(error_msg) from e

    except Exception as e:
        # 其他通用错误（如参数错误、网络问题）
        error_msg = f"【通用错误】{str(e)}"
        print(f"❌ {error_msg}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise Exception(error_msg) from e

    finally:
        # 确保资源释放（无论成功/失败）
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

    return result_sets


# -------------------------- 接口定义 --------------------------
@app.route('/users', methods=['GET', 'POST'])
def get_users():
    """用户数据接口（支持GET/POST）"""
    try:
        # 解析请求参数（兼容GET查询参数和POST数据）
        if request.method == 'GET':
            param1 = request.args.get('param1', '')  # 存储过程名
            param2 = request.args.get('param2', '')  # 存储过程参数
        else:
            # 优先解析JSON，其次解析表单数据
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        print(f"\n【接口请求】/users - param1: {param1}, param2: {param2}")
        # 调用存储过程获取数据
        result_data = get_multiple_result_sets(param1, param2)
        # 转换为JSON并返回（确保中文正常显示）
        return json.dumps(
            result_data,
            default=convert_datetime,
            ensure_ascii=False,
            indent=4
        ), 200, {'Content-Type': 'application/json; charset=utf-8'}

    except Exception as e:
        # 接口错误返回（统一格式）
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500


@app.route('/jsonService', methods=['GET', 'POST'])
def get_json():
    """通用JSON服务接口（支持GET/POST）"""
    try:
        # 解析参数（逻辑与/users一致）
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
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500


@app.route('/xmlService', methods=['GET', 'POST'])
def get_xml(encoding="utf-8"):
    """XML格式数据接口（支持GET/POST）"""
    try:
        # 解析参数
        if request.method == 'GET':
            param1 = request.args.get('param1', '')
            param2 = request.args.get('param2', '')
        else:
            data = request.get_json() or request.form.to_dict()
            param1 = data.get('param1', '')
            param2 = data.get('param2', '')

        print(f"\n【接口请求】/xmlService - param1: {param1}, param2: {param2}")
        result_data = get_multiple_result_sets(param1, param2)

        # 转换结果集为格式化XML
        root = ET.Element("ResultSets")
        root.set("generated_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        root.set("total_sets", str(len(result_data)))

        for set_idx, result_set in enumerate(result_data, 1):
            if not result_set:
                continue
            # 从第一行数据获取列名
            columns = list(result_set[0].keys()) if result_set else []
            # 创建结果集节点
            set_node = ET.SubElement(root, "ResultSet")
            set_node.set("id", str(set_idx))
            set_node.set("row_count", str(len(result_set)))
            set_node.set("column_count", str(len(columns)))

            # 添加列名节点
            cols_node = ET.SubElement(set_node, "Columns")
            for col in columns:
                ET.SubElement(cols_node, "Column").text = col

            # 添加行数据节点
            rows_node = ET.SubElement(set_node, "Rows")
            for row_idx, row in enumerate(result_set, 1):
                row_node = ET.SubElement(rows_node, "Row")
                row_node.set("index", str(row_idx))
                for col_idx, col_name in enumerate(columns):
                    cell_value = row[col_name]
                    # 处理特殊数据类型
                    if isinstance(cell_value, datetime):
                        cell_text = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    elif cell_value is None:
                        cell_text = ""
                    else:
                        cell_text = str(cell_value)
                    # 创建单元格节点
                    cell_node = ET.SubElement(row_node, "Cell")
                    cell_node.set("column", col_name)
                    cell_node.set("column_index", str(col_idx))
                    cell_node.text = cell_text

        # 格式化XML（增加缩进，去除空行）
        rough_xml = ET.tostring(root, encoding=encoding)
        pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent="  ", encoding=encoding)
        clean_xml = "\n".join([line for line in pretty_xml.decode(encoding).split("\n") if line.strip()])

        # 返回XML响应（设置正确的MIME类型）
        return Response(clean_xml, mimetype=f'application/xml; charset={encoding}')

    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500


# -------------------------- 服务启动 --------------------------
if __name__ == '__main__':
    print("=" * 60)
    print("【达梦API服务】启动中...")
    print(f"启动时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"运行目录：{os.getcwd()}")
    print("服务地址：")
    print(" - http://127.0.0.1:5000")
    print(" - http://192.168.0.191:5000（局域网地址）")
    print("可用接口：")
    print(" - GET/POST /users       ：用户数据接口（JSON）")
    print(" - GET/POST /jsonService  ：通用JSON服务接口")
    print(" - GET/POST /xmlService   ：XML格式数据接口")
    print("=" * 60)
    # 启动服务（允许局域网访问，开启调试模式便于排查）
    app.run(host='0.0.0.0', port=5000, debug=True)