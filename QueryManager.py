import requests
import sys
import pickle
import os
import time
import dateutil.parser
import sqlite3
import datetime
from PUBGModels import *


# This is a huge bag of dicks.
# This is a function that can only be used as a decorator for the QueryManager class, and should not be
# exposed to anything else. But, I can't figure out how to get it inside of the class.
def SQLCommit(func):
	def wrapper(manager, *args, **kwargs):
		try:
			val = func(manager, *args, **kwargs)
			manager.DBConnection.commit()
			return val
		except Exception as e:
			manager.DBConnection.rollback()
			raise e
		return None

	return wrapper

class QueryManager():
	apiURL = 'https://api.playbattlegrounds.com/shards/pc-na'

	def __init__(self, key, fp):
		assert key is not None and key != '', 'Needs a key'
		assert fp !='', 'Needs a file source'
		self.APIKey = key
		self.DBConnection = sqlite3.connect(fp)
		self.DBConnection.row_factory = sqlite3.Row
		self.Cursor = self.DBConnection.cursor()
		self._createTables()
		return

	# Public API
	def getPlayerById(self, id):
		self.Cursor.execute('select PlayerDim.Name from PlayerDim where PlayerDim.PlayerId = ?', (id, ))
		name = self.Cursor.fetchone()
		if name is not None:
			return self.getPlayer(name)

		player, _ = self._request(QueryManager.apiURL + '/players/{}'.format(id))
		player = Player.FromJSON(player)

		self._savePlayerToDB(player)
		return player

	def getPlayer(self, name):
		oldPlayer = self._getPlayerFromDB(name)
		if oldPlayer is not None and time.time() - oldPlayer.QueryTime < 900:
			return oldPlayer

		player, _ = self._request(QueryManager.apiURL + '/players?filter[playerNames]={}'.format(name))
		player = Player.FromJSON(player[0])  # Gets a list of players. Just need the one.

		if oldPlayer is not None:
			oldPlayer.update(player)
			player = oldPlayer

		self._savePlayerToDB(player)

		return player

	def getMatchDetails(self, matchID):
		match = self._getMatchFromDB(matchID)
		if match is not None:
			return match

		match, included = self._request(QueryManager.apiURL + '/matches/{}'.format(matchID))
		match = Match.FromJSON(match, included)

		self._saveMatchToDB(match)

		return match


	# Private
	def _createTables(self):
		script = ('''
			create table if not exists PlayerDim 
				(PlayerId, Name, QueryTime, ShardId, CreatedAt, UpdatedAt, PatchVersion, MatchComboKey);

			create table if not exists MatchBridge 
				(MatchComboKey, MatchKey, Number);

			create table if not exists MatchFact
				(MatchId, MapName, Duration, TelemetryUrl, CreatedAt, ShardId, TitleId, TeamComboKey, ParticipantComboKey);

			create table if not exists ParticipantBridge 
				(ParticipantComboKey, ParticipantKey);

			create table if not exists ParticipantFact
				(ParticipantId,PlayerId,Name,KillPlace,KillPoints,LastKillPoints,
				KillPointsDelta,WinPlace,WinPoints,LastWinPoints ,WinPointsDelta,DBNOs,
				TimeSurvived,Boosts,Heals,DeathType,Revives,Kills,HeadshotKills,
				KillStreaks,LongestKill,RoadKills,TeamKills,Assists,MostDamage,DamageDealt,
				RideDistance,VehicleDestroys,WalkDistance,WeaponsAcquired);

			create table if not exists TeamBridge 
				(TeamComboKey, TeamKey);

			create table if not exists TeamFact
				(TeamId,Rank,TeamNumber,Won,ParticipantComboKey);
				
			create table if not exists PositionEventFact
				(PlayerId, Time, Version, MatchId, Health, NumPlayersAlive, Ranking, TeamNumber, X, Y, Z
				,primary key (PlayerId, Time));
			
			create table if not exists AttackEventFact
				(AttackId, MatchId, Time, Version, AttackerId, AttackType, WeaponProfileId, VehicleType, VehicleId, 
					VehicleHealthPercent, FuelPercent
				,primary key (AttackId, MatchId));
				
			create table if not exists DamageEventFact
				(AttackId, MatchId, Time, Version, AttackerId, AttackerX, AttackerY, AttackerZ, AttackerHealth,
				 	VictimId, VictimX, VictimY, VictimZ, VictimHealth,  DamageReason, Damage, 
				primary key (AttackId, MatchId));
				
			create table if not exists KillEventFact
				(AttackId, MatchId, Time, Version, AttackerId, AttackerX, AttackerY, AttackerZ, AttackerHealth,
				 	VictimId, VictimX, VictimY, VictimZ, VictimHealth, Distance, DamageCauserName, DamageType,
				primary key (AttachId, MatchId));
				
			create table if not exists ItemUseEventFact
				(PlayerId, Time, Version, MatchId, Health, ItemId, Category, SubCategory,
					primary key (PlayerId, Time));
				
			create table if not exists CarePackageSpawnEventFact
				(MatchId, Time, Version, 
				
			''')
		self.Cursor.executescript(script)
		return

	def _getMatchFromDB(self, matchId):
		self.Cursor.execute('select * from MatchFact where MatchFact.MatchId = ?', (matchId, ))
		m = self.Cursor.fetchone()

		if m is None:
			return m

		match = Match.FromSQL(m)

		#Deserialize the participants.
		self.Cursor.execute('''
			select 
				ParticipantKey
			from ParticipantBridge
			where ParticipantComboKey = ?
		''', (match.ParticipantComboKey, ))

		pKeys = [r['ParticipantKey'] for r in self.Cursor.fetchall()]
		self.Cursor.execute('''
			select
				*
			from ParticipantFact
			where ParticipantId in ({})
		'''.format(','.join(['?'] * len(pKeys))), pKeys)
		match.Participants = set(Participant.FromSQL(p) for p in self.Cursor.fetchall())

		#Deserialize the teams.
		self.Cursor.execute('''
			select 
				TeamKey
		from TeamBridge
			where TeamComboKey = ?
		''', (match.TeamComboKey,))

		tKeys = [r['TeamKey'] for r in self.Cursor.fetchall()]
		self.Cursor.execute('''
			select
				*
			from TeamFact
			where TeamId in ({})
		'''.format(','.join(['?'] * len(tKeys))), tKeys)
		match.Teams = set(Team.FromSQL(t) for t in self.Cursor.fetchall())

		return match

	@SQLCommit
	def _saveMatchToDB(self, match):
		# Saving to MatchFact
		self.Cursor.execute('''
				select 
					1
				from MatchFact
				where MatchFact.MatchId = ?
				limit 1
		''', (match.MatchId, ))
		if self.Cursor.fetchone() is None:
			self.Cursor.execute('''
				insert into MatchFact
					values (?,?,?,?,?,?,?,?,?)
			''', match.asRow())
		else:
			self.Cursor.execute('''
				update MatchFact
				set MatchId = ?
					,MapName = ?
					,Duration = ?
					,TelemetryId = ?
					,CreatedAt = ?
					,ShardId = ?
					,TitleId = ?
					,TeamComboKey = ?
					,ParticipantComboKey = ?
				where MatchId = ?
			''', match.asRow() + (match.MatchId, ))

		#Saving to ParticipantBridge
		self.Cursor.execute('delete from ParticipantBridge where ParticipantBridge.ParticipantComboKey = ?',
							(match.ParticipantComboKey, ))
		bridgeRows = [(match.ParticipantComboKey, p.ParticipantId) for p in match.Participants]
		self.Cursor.executemany('insert into ParticipantBridge values (?,?)', bridgeRows)

		#Saving Participants
		for p in match.Participants:
			self._saveParticipantToDB(p)

		#Saving to TeamBridge
		self.Cursor.execute('delete from TeamBridge where TeamBridge.TeamComboKey = ?',
							(match.TeamComboKey, ))
		bridgeRows = [(match.TeamComboKey, t.TeamId) for t in match.Teams]
		self.Cursor.executemany('insert into TeamBridge values (?,?)', bridgeRows)

		#Saving Teams
		for t in match.Teams:
			self._saveTeamToDB(t)

		return True

	def _saveParticipantToDB(self, participant):
		self.Cursor.execute('''
				select 
					1
				from ParticipantFact
				where ParticipantFact.ParticipantId = ?
				limit 1
		''', (participant.ParticipantId, ))

		if self.Cursor.fetchone() is None:
			self.Cursor.execute('''
				insert into ParticipantFact
					values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			''', participant.asRow())
		else:
			self.Cursor.execute('''
				update MatchFact
				set ParticipantId = ?
					,PlayerId = ?
					,Name = ?
					,KillPlace = ?
					,KillPoints = ?
					,LastKillPoints = ?
					,KillPointsDelta = ?
					,WinPlace = ?
					,WinPoints = ?
					,LastWinPoints  = ?
					,WinPointsDelta = ?
					,DBNOs = ?
					,TimeSurvived = ?
					,Boosts = ?
					,Heals = ?
					,DeathType = ?
					,Revives = ?
					,Kills = ?
					,HeadshotKills = ?
					,KillStreaks = ?
					,LongestKill = ?
					,RoadKills = ?
					,TeamKills = ?
					,Assists = ?
					,MostDamage = ?
					,DamageDealt = ?
					,RideDistance = ?
					,VehicleDestroys = ?
					,WalkDistance = ?
					,WeaponsAcquired = ?			
				where ParticipantId = ?
			''', participant.asRow() + (participant.ParticipantId, ))

		return True

	def _saveTeamToDB(self, team):
		self.Cursor.execute('''
			select 
				1
			from TeamFact
			where TeamFact.TeamId = ?
			limit 1
		''', (team.TeamId, ))

		if self.Cursor.fetchone() is None:
			self.Cursor.execute('''
				insert into TeamFact
				values (?,?,?,?,?)
			''', team.asRow())
		else:
			self.Cursor.execute('''
				update TeamFact
				set TeamId = ?
					,Rank = ?
					,TeamNumber = ?
					,Won = ?
					,ParticipantComboKey = ?
				where TeamFact.TeamId = ?
			''', team.asRow() + (team.TeamId, ))

		return True

	def _getPlayerFromDB(self, name = None, id = None):
		assert name is not None or id is not None, 'Needs an id for the player'
		if name is not None:
			self.Cursor.execute('select * from PlayerDim where PlayerDim.Name = ? limit 1', (name,))
		else:
			self.Cursor.execute('select * from PlayerDim where PlayerDim.PlayerId = ? limit 1', (id,))

		p = self.Cursor.fetchone()
		if p is None:
			return p
		player = Player.FromSQL(p)

		self.Cursor.execute(('''
				select 
					MatchKey 
					,Number 
				from MatchBridge 
				where MatchBridge.MatchComboKey = ? 
				order by MatchBridge.Number desc
				'''), (player.MatchComboKey,))
		rows = self.Cursor.fetchall()
		matches = [r['MatchKey'] for r in rows]
		player.Matches = matches
		return player

	@SQLCommit
	def _savePlayerToDB(self, player):
		self.Cursor.execute(('''
						select 
							1 
						 from PlayerDim 
						 where PlayerDim.PlayerId = ? 
						 limit 1
						 '''), (player.PlayerId,))

		if self.Cursor.fetchone() is None:
			self.Cursor.execute('insert into PlayerDim values (?,?,?,?,?,?,?,?)', player.asRow())
		else:
			self.Cursor.execute(('''
							update PlayerDim 
							set PlayerId = ? 
								,Name = ? 
								,QueryTime = ? 
								,ShardId = ? 
								,CreatedAt = ? 
								,UpdatedAt = ? 
								,PatchVersion = ? 
								,MatchComboKey = ?
							where PlayerId = ?
							'''),  player.asRow() + (player.PlayerId, ))

		bridgeRows = [(player.MatchComboKey, m, i) for i, m in enumerate(reversed(player.Matches))]
		self.Cursor.execute('delete from MatchBridge where MatchBridge.MatchComboKey = ?', (player.MatchComboKey,))
		self.Cursor.executemany('insert into MatchBridge values (?,?,?)', bridgeRows)
		return True

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
	
	qm = QueryManager(key, 'data.db')
	player = qm.getPlayer('Kevdog25')
	for m in player.Matches:
		print(qm.getMatchDetails(m))

	return

if __name__ == '__main__':
	main()
