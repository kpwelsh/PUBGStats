import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from pymongo import MongoClient
from QueryManager import QueryManager
import numpy as np
import sys


def main():
	if len(sys.argv) < 2:
		print('Needs an API key as a command line arg.')
		return
	key = sys.argv[1]

	db = MongoClient()['PUBGStats']
	qm = QueryManager(key, db)

	player = qm.getPlayer('Kevdog25')
	player2 = qm.getPlayer('LargeDingDong')

	times = [(e['_D'], (e['character']['location']['x'], e['character']['location']['y'], e['character']['location']['z']))
			  				for e in db.Events.find(
			{
				'_T' : 'LogPlayerPosition',
			 	'MatchId' : player.Matches[0],
				'$or': [
					{'character.accountId' : player.PlayerId},
					{'character.accountId' : player2.PlayerId}
				]
			})]


	times = sorted(times, reverse= True)

	fig, ax = plt.subplots()
	ax.plot([t[1][0] for t in times], [t[1][1] for t in times])
	ax.set_ylim([0,816000])
	ax.set_xlim([0,816000])
	img = mpimg.imread('./Pics/Desert_Main_lowres.jpg')
	ax.imshow(img, extent = [0,816000,0,816000])

	[ax.spines[spine].set_visible(False) for spine in ax.spines]
	ax.set_xticklabels([]);
	ax.set_yticklabels([]);
	ax.tick_params(bottom = False, top = False, left = False, right = False)
	plt.show()


	return


if __name__ == '__main__':
	main()
