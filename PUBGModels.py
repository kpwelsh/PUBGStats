import time

# I hate dict notation. So I turned them into objects
class SQLRow():
	def __init__(self):
		return

	@classmethod
	def FromSQL(cls, row):
		p = Player()
		for key in row.keys():
			p.__dict__[key] = row[key]
		return p


class Player(SQLRow):
	@classmethod
	def FromJSON(cls, dict):
		p = Player()
		p.PlayerId = dict['id']
		p.MatchComboKey = 'Player:{}'.format(p.PlayerId)
		p.Matches = [m['id'] for m in dict['relationships']['matches']['data']]
		p.ShardId = dict['attributes']['shardId']
		p.CreatedAt = dict['attributes']['createdAt']
		p.UpdatedAt = dict['attributes']['updatedAt']
		p.PatchVersion = dict['attributes']['patchVersion']
		p.Name = dict['attributes']['name']
		p.QueryTime = time.time()
		return p

	def __init__(self):
		self.PlayerId = None
		self.MatchComboKey = None
		self.Matches = None
		self.ShardId = None
		self.CreatedAt = None
		self.UpdatedAt = None
		self.PatchVersion = None
		self.Name = None
		self.QueryTime = None
		return

	def update(self, other):
		self.ShardId = other.ShardId
		self.CreatedAt = other.CreatedAt
		self.UpdatedAt = other.UpdatedAt
		self.PatchVersion = other.PatchVersion
		self.Name = other.Name
		self.Matches = [m for m in other.Matches if m not in self.Matches] + self.Matches
		self.QueryTime = time.time()
		return


	def matchComboKey(self):
		return 'Player:{}'.format(self.Id)

	def asRow(self):
		'''(PlayerId, Name, QueryTime, ShardId, CreatedAt, UpdatedAt, PatchVersion, MatchComboKey)'''
		return (self.PlayerId, self.Name, self.QueryTime, self.ShardId, self.CreatedAt, self.UpdatedAt, self.PatchVersion,
				self.MatchComboKey)


class Match(SQLRow):
	@classmethod
	def FromJSON(cls, dict, included):
		m = Match()
		m.MatchId = dict['id']
		m.CreatedAt = dict['attributes']['createdAt']
		m.Duration = dict['attributes']['duration']
		m.GameMode = dict['attributes']['gameMode']
		m.MapName = dict['attributes']['mapName']
		m.ShardId = dict['attributes']['shardId']
		m.TitleId = dict['attributes']['titleId']

		m.Teams = set()
		m.TeamComboKey = 'Match:{}'.format(m.MatchId)
		m.Participants = set()
		m.ParticipantComboKey = 'Match:{}'.format(m.MatchId)

		telemetryId = dict['relationships']['assets']['data'][0]['id']
		if len(dict['relationships']['assets']['data']) > 1:
			print('Hey, I found a match that has a few more assets')

		for item in included:
			if item['type'] == 'participant':
				m.Participants.add(Participant.FromJSON(item))
			elif item['type'] == 'roster':
				m.Teams.add(Team.FromJSON(item))
			elif item['id'] == telemetryId:
				m.TelemetryUrl = item['attributes']['URL']
		return m

	def __init__(self):
		self.MatchId = None
		self.CreatedAt = None
		self.Duration = None
		self.GameMode = None
		self.MapName = None
		self.ShardId = None
		self.TitleId = None
		self.TelemetryUrl = None

		self.Teams = {}
		self.TeamComboKey = None
		self.Participants = {}
		self.ParticipantComboKey = None

		return

	def asRow(self):
		'''(MatchId, MapName, Duration, TelemetryUrl, CreatedAt, ShardId, TitleId, TeamComboKey, ParticipantComboKey)'''
		return (self.MatchId, self.MapName, self.Duration, self.TelemetryUrl, self.CreatedAt, self.ShardId, self.TitleId,
				self.TeamComboKey, self.ParticipantComboKey)


class Participant(SQLRow):

	@classmethod
	def FromJSON(cls, dict):
		p = Participant()
		p.ParticipantId = dict['id']
		p.PlayerId = dict['attributes']['stats']['playerId']
		p.Name = dict['attributes']['stats']['name']

		p.KillPlace = dict['attributes']['stats']['killPlace']
		p.KillPoints = dict['attributes']['stats']['killPoints']
		p.LastKillPoints = dict['attributes']['stats']['lastKillPoints']
		p.KillPointsDelta = dict['attributes']['stats']['killPointsDelta']
		p.WinPlace = dict['attributes']['stats']['winPlace']
		p.WinPoints = dict['attributes']['stats']['winPoints']
		p.LastWinPoints = dict['attributes']['stats']['lastWinPoints']
		p.WinPointsDelta = dict['attributes']['stats']['winPointsDelta']

		p.DBNOs = dict['attributes']['stats']['DBNOs']
		p.TimeSurvived = dict['attributes']['stats']['timeSurvived']
		p.Boosts = dict['attributes']['stats']['boosts']
		p.Heals = dict['attributes']['stats']['heals']
		p.DeathType = dict['attributes']['stats']['deathType']
		p.Revives = dict['attributes']['stats']['revives']

		p.Kills = dict['attributes']['stats']['kills']
		p.HeadshotKills = dict['attributes']['stats']['headshotKills']
		p.KillStreaks = dict['attributes']['stats']['killStreaks']
		p.LongestKill = dict['attributes']['stats']['longestKill']
		p.RoadKills = dict['attributes']['stats']['roadKills']
		p.TeamKills = dict['attributes']['stats']['teamKills']
		p.Assists = dict['attributes']['stats']['assists']
		p.MostDamage = dict['attributes']['stats']['mostDamage']
		p.DamageDealt = dict['attributes']['stats']['damageDealt']

		p.RideDistance = dict['attributes']['stats']['rideDistance']
		p.VehicleDestroys = dict['attributes']['stats']['vehicleDestroys']
		p.WalkDistance = dict['attributes']['stats']['walkDistance']
		p.WeaponsAcquired = dict['attributes']['stats']['weaponsAcquired']
		return p

	def __init__(self):
		self.ParticipantId = None
		self.PlayerId = None
		self.Name = None
		self.KillPlace = None
		self.KillPoints = None
		self.LastKillPoints = None
		self.KillPointsDelta = None
		self.WinPlace = None
		self.WinPoints = None
		self.LastWinPoints = None
		self.WinPointsDelta = None
		self.DBNOs = None
		self.TimeSurvived = None
		self.Boosts = None
		self.Heals = None
		self.DeathType = None
		self.Revives = None
		self.Kills = None
		self.HeadshotKills = None
		self.KillStreaks = None
		self.LongestKill = None
		self.RoadKills = None
		self.TeamKills = None
		self.Assists = None
		self.MostDamage = None
		self.DamageDealt = None
		self.RideDistance = None
		self.VehicleDestroys = None
		self.WalkDistance = None
		self.WeaponsAcquired = None
		return

	def asRow(self):
		'''
		(ParticipantId,PlayerId,Name,KillPlace,KillPoints,LastKillPoints,
		KillPointsDelta,WinPlace,WinPoints,LastWinPoints ,WinPointsDelta,DBNOs,
		TimeSurvived,Boosts,Heals,DeathType,Revives,Kills,HeadshotKills,
		KillStreaks,LongestKill,RoadKills,TeamKills,Assists,MostDamage,DamageDealt,
		RideDistance,VehicleDestroys,WalkDistance,WeaponsAcquired)'''
		return (self.ParticipantId,self.PlayerId,self.Name,self.KillPlace,self.KillPoints,self.LastKillPoints,
			self.KillPointsDelta,self.WinPlace,self.WinPoints,self.LastWinPoints ,self.WinPointsDelta,self.DBNOs,
			self.TimeSurvived,self.Boosts,self.Heals,self.DeathType,self.Revives,self.Kills,self.HeadshotKills,
			self.KillStreaks,self.LongestKill,self.RoadKills,self.TeamKills,self.Assists,self.MostDamage,self.DamageDealt,
			self.RideDistance,self.VehicleDestroys,self.WalkDistance,self.WeaponsAcquired)


class Team(SQLRow):

	@classmethod
	def FromJSON(cls, dict):
		t = Team()
		t.TeamId = dict['id']
		t.Rank = dict['attributes']['stats']['rank']
		t.TeamNumber = dict['attributes']['stats']['teamId']
		t.Won = dict['attributes']['won']
		t.Participants = set(p['id'] for p in dict['relationships']['participants']['data'])
		t.ParticipantComboKey = 'Team:{}'.format(t.TeamId)
		return t

	def __init__(self):
		self.TeamId = None
		self.Rank = None
		self.TeamNumber = None
		self.Won = None
		self.Participants = None
		self.ParticipantComboKey = None
		return

	def asRow(self):
		'''(TeamId,Rank,TeamNumber,Won,ParticipantComboKey)'''
		return (self.TeamId,self.Rank,self.TeamNumber,self.Won,self.ParticipantComboKey)


class Asset(SQLRow):

	@classmethod
	def FromJSON(cls, dict):
		a = Asset()
		a.AssetId = dict['id']
		a.CreatedAt = dict['attributes']['createdAt']
		a.Name = dict['attributes']['name']
		a.URL = dict['attributes']['URL']
		a.Value = None
		return a

	def __init__(self):
		self.AssetId = None
		self.CreatedAt = None
		self.Name = None
		self.URL = None
		self.Data = None
		return

	def asRow(self):
		'''(AssetId, CreatedAt, Name, URL, Data)'''
		return (self.AssetId, self.CreatedAt, self.Name, self.URL, self.Data)

	def request(self):
		header = {
			"Accept": "application/vnd.api+json"
		}
		r = requests.get(self.URL, headers=header)
		self.Value = r.json()
		return
