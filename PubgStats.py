import requests
import sys
import pickle
import os
import time
import dateutil.parser


# I hate dict notation. So I turned them into objects
class Player:
	def __init__(self, dict):
		self.Id = dict['id']
		self.Matches = [m['id'] for m in dict['relationships']['matches']['data']]
		self.Stats = dict['attributes']['stats']
		self.ShardId = dict['attributes']['shardId']
		self.CreatedAt = dateutil.parser.parse(dict['attributes']['createdAt'])
		self.UpdatedAt = dateutil.parser.parse(dict['attributes']['updatedAt'])
		self.PatchVersion = dict['attributes']['patchVersion']
		self.Name = dict['attributes']['name']
		self.QueryTime = time.time()
		return
		
	def update(self, other):
		self.Stats = other.Stats
		self.ShardId = other.ShardId
		self.CreatedAt = other.CreatedAt
		self.UpdatedAt = other.UpdatedAt
		self.PatchVersion = other.PatchVersion
		self.Name = other.Name

		self.Matches = [m for m in other.Matches if m not in self.Matches] + self.Matches

		self.setTime()
		return
		
	def setTime(self):
		self.QueryTime = time.time()
		return

class Match:
	def __init__(self, dict, included):

		self.Id = dict['id']
		self.CreatedAt = dateutil.parser.parse(dict['attributes']['createdAt'])
		self.Duration = dict['attributes']['duration']
		self.GameMode = dict['attributes']['gameMode']
		self.MapName = dict['attributes']['mapName']
		self.ShardId = dict['attributes']['shardId']
		self.TitleId = dict['attributes']['titleId']
		self.TelemetryId = dict['relationships']['assets']['data'][0]['id']
		if len(dict['relationships']['assets']['data']) > 1:
			print('Hey, I found a match that has a few more assets')

		self.Teams = set()
		self.Participants = {}
		teamIds = set()
		for item in included:
			if item['type'] == 'participant' and item['id'] not in participantIds:
				participantIds.add(item['id'])
				self.Participants.add(Participant(item))
			elif item['type'] == 'roster' and item['id'] not in teamIds:
				teamIds.add(item['id'])
				self.Teams.add(Team(item))
		return

class Participant:
	def __init__(self, dict):
		self.Id = dict['id']
		self.PlayerId = dict['attributes']['stats']['playerId']
		self.Stats = dict['attributes']['stats']
		return

class Team:
	def __init__(self, dict):
		self.Id = dict['id']
		self.Rank = dict['attributes']['stats']['rank']
		self.TeamNumber = dict['attributes']['stats']['teamId']
		self.Won = dict['attributes']['won']
		self.Participants = set(p['id'] for p in dict['relationships']['participants']['data'])
		return


class QueryManager():
	apiURL = 'https://api.playbattlegrounds.com/shards/pc-na'
	def __init__(self, key, fp):
		assert key is not None and key != '', 'Needs a key'
		assert fp !='', 'Needs a file source'
		self.APIKey = key
		self.DBLoc = fp
		if os.path.isfile(fp):
			with open(fp,'rb') as fin:
				self.CachedDB = pickle.load(fin)
		else:
			self.CachedDB = { 
				'Players': {},
				'Matches': {},
				'Telemetry': {}
			}
		return
		
	def getPlayer(self, name):
		oldPlayer = None
		if name in self.CachedDB['Players']:
			oldPlayer = self.CachedDB['Players'][name]
			if time.time() - oldPlayer.QueryTime < 900:
				return oldPlayer
			
		player, _ = self.request(QueryManager.apiURL + '/players?filter[playerNames]={}'.format(name))
		player = Player(player[0]) # Gets a list of players. Just need the one.
		
		if oldPlayer is not None:
			oldPlayer.update(player)
		else:
			self.CachedDB['Players'][name] = player
			self.CachedDB['PlayerIdToName'][player.Id] = name
			
		return player

	def getPlayerById(self, id):
		if id in self.CachedDB['PlayerIdToName']:
			return self.getPlayer(self.CachedDB['PlayerIdToName'][id])

		player, _ = self.request(QueryManager.apiURL + '/players/{}'.format(id))
		player = Player(player)
		self.CachedDB['Players'][player.Name] = player
		self.CachedDB['PlayerIdToName'][player.Id] = player.Name

		return player

	def getMatchDetails(self, matchID):
		if matchID in self.CachedDB['Matches']:
			return self.CachedDB['Matches'][matchID]
		match = Match(*self.request(QueryManager.apiURL + '/matches/{}'.format(matchID)))
		self.CachedDB['Matches'][matchID] = match
		return match
		
	def request(self, reqUrl):
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
		
	def saveDB(self):
		if not self.CachedDB:
			return
		with open(self.DBLoc, 'wb+') as fout:
			pickle.dump(self.CachedDB, fout)
		return
	

def main():
	if len(sys.argv) < 2:
		print('Needs an API key as a command line arg.')
		return
	key = sys.argv[1]
	
	qm = QueryManager(key, 'data.db')
	player = qm.getPlayer('Kevdog25')
	for m in player.Matches:
		print(m)

	match = qm.getMatchDetails(m)
	print(match.CreatedAt)

	qm.saveDB()
	return

if __name__ == '__main__':
	main()
