import dmPython
import os
from flask import Flask, jsonify, Response, request
import json
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
    """调用达梦存储过程（兼容不同dmPython版本的错误格式）"""
    # 加密库检查
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

    # 数据库连接参数
    conn_params = {
        'server': '192.168.0.191',  # 已更新为日志中的服务器地址
        'user': 'JZX',
        'password': 'XFgs@345',
        'port': 5236,
        'autoCommit': True
    }
    result_sets = []
    conn = None
    cursor = None

    try:
        print("\n【数据库操作】开始连接达梦数据库...")
        print(f"连接参数：server={conn_params['server']}, port={conn_params['port']}, user={conn_params['user']}")

        # 连接阶段错误捕获（兼容不同dmPython版本）
        try:
            conn = dmPython.connect(**conn_params)
        except dmPython.DatabaseError as e:
            # 提取错误信息（兼容errno属性或args元组）
            try:
                # 尝试获取错误码（部分版本用errno）
                error_code = e.errno
                error_desc = e.strerror
            except AttributeError:
                # 若没有errno，从args提取（通常args[0]是错误码，args[1]是描述）
                error_code = e.args[0] if len(e.args) > 0 else '未知'
                error_desc = e.args[1] if len(e.args) > 1 else str(e)

            error_detail = (
                f"达梦连接失败 [错误码: {error_code}]\n"
                f"错误描述: {error_desc}\n"
                f"可能原因: \n"
                f"  1. 数据库地址/端口错误（当前：{conn_params['server']}:{conn_params['port']}\n"
                f"  2. 用户名/密码错误（当前用户：{conn_params['user']}\n"
                f"  3. 数据库服务未启动或端口未开放\n"
                f"  4. 加密库版本与数据库不兼容"
            )
            print(f"❌ {error_detail}")
            raise Exception(error_detail) from e

        print("✅ 数据库连接成功（加密模块加载正常）")
        cursor = conn.cursor()

        # 检查存储过程名
        if not strSp:
            raise Exception("【参数错误】存储过程名（param1）不能为空")

        # 调用存储过程（兼容错误格式）
        print(f"调用存储过程：{strSp}，参数：{strParam}")
        try:
            cursor.callproc(strSp, (strParam,))
        except dmPython.DatabaseError as e:
            # 同样兼容错误码提取
            try:
                error_code = e.errno
                error_desc = e.strerror
            except AttributeError:
                error_code = e.args[0] if len(e.args) > 0 else '未知'
                error_desc = e.args[1] if len(e.args) > 1 else str(e)

            error_detail = (
                f"存储过程调用失败 [错误码: {error_code}]\n"
                f"错误描述: {error_desc}\n"
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
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                result_sets.append(rows)
                print(f"✅ 获取结果集 {set_index}：{len(rows)} 行数据")
                set_index += 1
            if not cursor.nextset():
                break

        conn.commit()
        print(f"✅ 所有结果集处理完成（共 {len(result_sets)} 个）")
        return result_sets

    except Exception as e:
        print(f"❌ 处理中断：{str(e)}")
        if conn:
            try:
                conn.rollback()
                print("ℹ️  事务已回滚")
            except:
                pass
        raise

    finally:
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


# 接口定义
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
            'message': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'param1': param1,
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

        # 生成XML
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