import dmPython
import pandas as pd

import tkinter as tk
from tkinter import messagebox

# 创建主窗口
root = tk.Tk()
root.title("Hello World")
root.geometry("300x200")

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

        # 获取第一个结果集
        result = cursor.fetchall()
        if result:
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            # 转换为DataFrame便于处理
            df = pd.DataFrame(result, columns=columns)
            result_sets.append(df)

        # 循环获取后续所有结果集
        while cursor.nextset():
            result = cursor.fetchall()
            if result:
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                result_sets.append(df)

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


if __name__ == "__main__":
    # 获取所有结果集
    tables = get_multiple_result_sets()

    # 处理每个结果集
    for i, table in enumerate(tables, 1):
        print(f"\n第{i}个结果集:")
        label = tk.Label(root, text=f"\n第{i}个结果集:", font=("Arial", 20))
        label.pack(expand=True)
        print(table)

    # 显示窗口
    root.mainloop()
