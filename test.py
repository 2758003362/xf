# import pymssql
import dmPython

import tkinter as tk
from tkinter import messagebox

# 创建主窗口
root = tk.Tk()
root.title("Hello World")
root.geometry("300x200")

def get_connection():
    """创建数据库连接"""
    try:
        # 创建连接
        # conn = pymssql.connect(server, user, password, database)
        # conn = pymssql.connect(server = '192.168.0.102',user = 'sa',password = 'wangwf',database = 'TestDB',as_dict = True,tds_version = '7.0')

        conn_params = {
            'server': '192.168.0.191',  # 服务器地址
            'user': 'JZX',  # 用户名
            'password': 'XFgs@345',  # 密码
            'port': 5236,  # 端口号，默认5236
            'autoCommit': True  # 是否自动提交
        }

        conn = dmPython.connect(**conn_params)

        label = tk.Label(root, text="数据库连接成功", font=("Arial", 20))
        label.pack(expand=True)
        return conn
    except Exception as e:
        label = tk.Label(root, text="数据库连接失败" + str(e), font=("Arial", 20))
        label.pack(expand=True)
        return None

conn = get_connection()

try:
    # 创建游标
    cursor = conn.cursor()

    # 执行查询
    cursor.execute("SELECT * FROM JZX.CDPORT")

    # 获取列名
    columns = [column[0] for column in cursor.description]
    print("列名：", columns)

    # 获取所有记录
    for row in cursor:
        print(row[0])

except Exception as e:
    print(f"查询失败：{str(e)}")
finally:
    cursor.close()

# 添加标签
label = tk.Label(root, text="Hello World!", font=("Arial", 20))
label.pack(expand=True)

# 显示窗口
root.mainloop()