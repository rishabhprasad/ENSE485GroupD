from flask import Flask, request,render_template
import psycopg2

# connect to your postgresql database
try: 
    conn = psycopg2.connect(database="ENSE485", user="aqicn",  
    password="GuiltySpark343", host="localhost")
    print("connected")
except:
    print ("I am unable to connect to the database")
mycursor =conn.cursor()
app = Flask(__name__)

@app.route('/')
def index():
    
    return render_template('index.html')

@app.route('/fetchTable', methods=['POST','GET'])
def fetchTable():
    cityName = request.args.get('city')
    mycursor.execute(f"SELECT * FROM airquality WHERE specie = 'pm25' AND city = '{cityName}'")
    data = mycursor.fetchall()

    mycursor.execute(f"SELECT * FROM aq_predictions WHERE city = '{cityName}' LIMIT 1")
    prediction = mycursor.fetchall()


    return render_template('fetchTable.html', data=data, prediction=prediction)
