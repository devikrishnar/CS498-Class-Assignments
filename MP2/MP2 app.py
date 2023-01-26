from flask import Flask, redirect, url_for, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def hello():
    val = '0'
    json_val = request.get_json()
    if request.method == 'POST':
        val = str(json_val['num'])
        with open('valuefile.txt', 'w') as f:
            f.write(val)
        return val
    if request.method == 'GET':
        with open('valuefile.txt', 'r') as f:
            for line in f:
                for word in line.split():
                    return word

if __name__ == '__main__':
   app.run(debug = True)