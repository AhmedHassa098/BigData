from flask import Flask, render_template, request
import pickle
import numpy as np

model = pickle.load(open('car.pkl', 'rb'))

app = Flask(__name__)



@app.route('/')
def man():
    return render_template('PredictionPage.html')


@app.route('/predict', methods=['POST'])
def home():
    Value1 = request.form['Box1']
    Value2 = request.form['Box2']
    Value3 = request.form['Box3']
    Value4 = request.form['Box4']
    arr = np.array([[Value1, Value2, Value3, Value4]])
    pred = model.predict(arr)
    return render_template('Result.html', data=pred)


if __name__ == "__main__":
    app.run(debug=True)















