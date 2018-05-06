import time
from dateutil import parser


def toDict(obj, classKey=None):
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = toDict(v, classKey)
        return data
    elif hasattr(obj, "_ast"):
        return toDict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [toDict(v, classKey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, toDict(value, classKey))
            for key, value in obj.__dict__.items()
            if not callable(value) and not key.startswith('_')])
        if classKey is not None and hasattr(obj, "__class__"):
            data[classKey] = obj.__class__.__name__
        return data
    else:
        return obj

class Player():
	@classmethod
	def FromBSON(cls, dict):
		p = Player()
		for key,v in dict.items():
			p.__dict__[key] = v
		return p

	@classmethod
	def FromJSON(cls, dict):
		p = Player()
		p.PlayerId = dict['id']
		p.Matches = [m['id'] for m in dict['relationships']['matches']['data']]
		p.ShardId = dict['attributes']['shardId']
		p.CreatedAt = time.mktime(parser.parse(dict['attributes']['createdAt']).timetuple())
		p.UpdatedAt = time.mktime(parser.parse(dict['attributes']['updatedAt']).timetuple())
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


class Match():
	@classmethod
	def FromBSON(cls, dict):
		m = Match()
		for key,v in dict.items():
			m.__dict__[key] = v
		m.Teams = set(Team.FromBSON(t) for t in m.Teams)
		m.Participants = set(Participant.FromBSON(t) for t in m.Participants)
		return m

	@classmethod
	def FromJSON(cls, dict, included):
		m = Match()
		m.MatchId = dict['id']
		m.CreatedAt = time.mktime(parser.parse(dict['attributes']['createdAt']).timetuple())
		m.Duration = dict['attributes']['duration']
		m.GameMode = dict['attributes']['gameMode']
		m.MapName = dict['attributes']['mapName']
		m.ShardId = dict['attributes']['shardId']
		m.TitleId = dict['attributes']['titleId']

		m.Teams = set()
		m.Participants = set()

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

		return

class Participant():

	@classmethod
	def FromBSON(cls, dict):
		p = Participant()
		for key,v in dict.items():
			p.__dict__[key] = v
		return p

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


class Team():
	@classmethod
	def FromBSON(cls, dict):
		t = Team()
		for key in dict.keys():
			t.__dict__[key] = dict[key]
		return t


	@classmethod
	def FromJSON(cls, dict):
		t = Team()
		t.TeamId = dict['id']
		t.Rank = dict['attributes']['stats']['rank']
		t.TeamNumber = dict['attributes']['stats']['teamId']
		t.Won = dict['attributes']['won']
		t.Participants = set(p['id'] for p in dict['relationships']['participants']['data'])
		return t

	def __init__(self):
		self.TeamId = None
		self.Rank = None
		self.TeamNumber = None
		self.Won = None
		self.Participants = None
		return
