from flask import Flask, jsonify, request

# 创建Flask应用实例
app = Flask(__name__)

# 模拟数据库 - 存储用户数据
users = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]


# 1. 获取所有用户 (GET请求)
@app.route('/users', methods=['GET'])
def get_users():
    return jsonify({"users": users})


# 2. 获取单个用户 (GET请求，带参数)
@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    user = next((u for u in users if u['id'] == user_id), None)
    if user:
        return jsonify({"user": user})
    return jsonify({"error": "用户不存在"}), 404


# 3. 创建新用户 (POST请求)
@app.route('/users', methods=['POST'])
def create_user():
    # 检查请求数据是否符合要求
    if not request.json or 'name' not in request.json:
        return jsonify({"error": "姓名为必填项"}), 400

    # 创建新用户对象
    new_user = {
        "id": len(users) + 1,
        "name": request.json['name'],
        "age": request.json.get('age', 0)  # 年龄为可选参数
    }
    users.append(new_user)
    return jsonify({"user": new_user}), 201  # 201表示创建成功


# 4. 更新用户信息 (PUT请求)
@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    user = next((u for u in users if u['id'] == user_id), None)
    if not user:
        return jsonify({"error": "用户不存在"}), 404

    if not request.json:
        return jsonify({"error": "无效的请求数据"}), 400

    # 更新用户信息（只更新提供的字段）
    user['name'] = request.json.get('name', user['name'])
    user['age'] = request.json.get('age', user['age'])
    return jsonify({"user": user})


# 5. 删除用户 (DELETE请求)
@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    global users
    user = next((u for u in users if u['id'] == user_id), None)
    if not user:
        return jsonify({"error": "用户不存在"}), 404

    users = [u for u in users if u['id'] != user_id]
    return jsonify({"message": "用户已删除"}), 200


# 启动服务
if __name__ == '__main__':
    # 允许外部访问，端口为5000，开启调试模式
    app.run(host='0.0.0.0', port=5000, debug=True)