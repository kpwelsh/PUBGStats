import requests
import sys
import pickle
import os
import time
import dateutil.parser
import pymongo
import datetime
import numpy as np
from PUBGModels import *


class QueryManager():
	apiURL = 'https://api.playbattlegrounds.com/shards/pc-na'

	def __init__(self, key, db):
		assert key is not None and key != '', 'Needs a key'
		assert db is not None, 'Needs a db source'
		self.APIKey = key
		self.DB = db
		return

	# Public API
	def getPlayer(self, name = None, id = None):
		assert id is not None or name is not None, 'Gimmie something, bruh.'
		query = {}
		if id is not None:
			query['PlayerId'] = id
		if name is not None:
			query['Name'] = name
		oldPlayer = self.DB.Players.find_one(query)

		if oldPlayer is not None:
			oldPlayer = Player.FromBSON(oldPlayer)
			if time.time() - oldPlayer.QueryTime < 900:
				return oldPlayer

		if id is not None:
			player, _ = self._request(QueryManager.apiURL + '/players/{}'.format(id))
		else:
			player, _ = self._request(QueryManager.apiURL + '/players?filter[playerNames]={}'.format(name))
			player = player[0]

		player = Player.FromJSON(player)

		if oldPlayer is not None:
			oldPlayer.update(player)
			player = oldPlayer

		self.DB.Players.update(query, toDict(player), upsert = True)
		return player

	def getMatchDetails(self, matchID):
		match = self.DB.Matches.find_one({'MatchId' : matchID})
		if match is not None:
			return match

		match, included = self._request(QueryManager.apiURL + '/matches/{}'.format(matchID))
		match = Match.FromJSON(match, included)

		self.DB.Matches.update({'MatchId' : matchID}, toDict(match), upsert = True)

		return match

	def cacheTelemetryData(self, match):

		if self.DB.Events.find_one({'MatchId' : match.MatchId}) is not None:
			print(self.DB.Events.find_one({'MatchId' : match.MatchId}))
			return

		header = {
			'Accept' : 'application/vnd.api+json'
		}
		r = requests.get(match.TelemetryUrl, headers = header).json()
		for e in r:
			e['MatchId'] = match.MatchId
			e['_D'] = time.mktime(dateutil.parser.parse(e['_D']).timetuple())
		self.DB.Events.insert_many(r)

		return

	def _request(self, reqUrl):
		header = {
		  "Authorization": "Bearer {}".format(self.APIKey),
		  "Accept": "application/vnd.api+json"
		}
		r = requests.get(reqUrl, headers=header)
		pubgResponse = r.json()
		if 'errors' in pubgResponse:
			raise Exception(str(pubgResponse['errors']))
		
		if 'included' in pubgResponse:
			return pubgResponse['data'], pubgResponse['included']
		return pubgResponse['data'], []


def main():
	if len(sys.argv) < 2:
		print('Needs an API key as a command line arg.')
		return
	key = sys.argv[1]

	db = pymongo.MongoClient()['PUBGStats']
	qm = QueryManager(key, db)

	player = qm.getPlayer('Kevdog25')

	times = [(e['_D'], (e['character']['location']['x'], e['character']['location']['y'], e['character']['location']['z']))
			  				for e in db.Events.find({'_T' : 'LogPlayerPosition',
											  'MatchId' : player.Matches[0],
											  'character.accountId' : player.PlayerId})]
	times = sorted(times)
	diff = []
	for i,t in enumerate(times[1:]):
		diff.append(t[0] - times[i])

	print(np.mean(diff))


	return

if __name__ == '__main__':
	main()
