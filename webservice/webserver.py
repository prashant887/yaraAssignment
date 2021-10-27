from flask import Flask,request,jsonify
from dbOps import dbOps

app=Flask(__name__)

@app.route("/get_hist_tab",methods=['GET'])
def getHistTab():
    db=dbOps()
    result=db.getHistTab()
    print(result)
    return result

@app.route("/get_hist",methods=['GET'])
def getHists():
    db=dbOps()
    result=db.getHist()
    print(result)
    return jsonify(result.get('result')),result.get('code')

@app.route("/get_loc_details",methods=['GET'])
def getLocDetails():
    req=request.get_json(force=True)
    db=dbOps()
    res=db.getLocDetails(req)
    return jsonify(res.get('result')),res.get('code')

@app.route("/add_loc_details",methods=['POST'])
def addLocDetails():
    req=request.get_json(force=True)
    db=dbOps()
    res=db.addLocDetails(req)
    return jsonify(res.get('result')),res.get('code')

@app.route("/update_loc_details",methods=['PUT'])
def updLocDetails():
    req=request.get_json(force=True)
    db=dbOps()
    res=db.updLocDetails(req)
    return jsonify(res.get('result')),res.get('code') 

@app.route("/delete_loc_details",methods=['DELETE'])
def delLocDetails():
    req=request.get_json(force=True)
    db=dbOps()
    res=db.delLocDetails(req)
    return jsonify(res.get('result')),res.get('code')


if __name__=="__main__":
    app.run(host='0.0.0.0',port=5025,debug=True)