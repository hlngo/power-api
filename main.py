from flask import Flask, request, current_app
from flask_restful import Resource, Api
from flask_restful.utils import cors

from src.power_data import PowerData
from src.zone_data import ZoneData, ZoneDataByDate
from src.afdd import Afdd


app = Flask(__name__)
api = Api(app, decorators=[cors.crossdomain(origin='*')])


class Root(Resource):
    def get(self):
        return current_app.send_static_file('index.html')

#api.add_resource(ZoneDataByDate, '/api/ZoneData/<string:date>')
api.add_resource(ZoneData, '/api/ZoneData')
api.add_resource(PowerData, '/api/PowerData/<int:resource_id>')
api.add_resource(Afdd, '/api/afdd/<int:resource_id>')
api.add_resource(Root, '/api/')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
