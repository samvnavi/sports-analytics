#classes for each player type

class Player:
    def __init__(self,name,games,team,tournaments):
        self.name = name
        self.games = games
        self.team = team
        self.tournaments = tournaments

class Bowler(Player):
    def __init__(self,name,games,team,tournaments,wickets):
        super().__init__(name,games,team,tournaments)
        self.wickets = wickets

class Batsman(Player):
    def __init__(self,name,games,team,tournaments,total_runs):
        super().__init__(name,games,team,tournaments)
        self.total_runs = total_runs
