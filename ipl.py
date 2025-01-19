from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine,Column,Integer,VARCHAR,Date,func,or_,and_,desc
from sqlalchemy.orm import sessionmaker
import csv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from operator import itemgetter

Base = declarative_base()
class ipl(Base):
    __tablename__ = 'cricket'
    sl_no = Column(Integer, primary_key=True)
    id = Column(Integer)
    innings = Column(Integer)
    batting_team = Column(VARCHAR(100))
    bowling_team = Column(VARCHAR(100))
    over = Column(Integer)
    ball_no = Column(Integer)
    batter = Column(VARCHAR(100))
    bowler = Column(VARCHAR(100))
    non_striker = Column(VARCHAR(100))
    batsman_runs = Column(Integer)
    extra_runs = Column(Integer)
    total_runs = Column(Integer)
    extras_type = Column(VARCHAR(100))
    is_wicket = Column(VARCHAR(100))
    player_dismissed = Column(VARCHAR(100))
    dismissal_kind = Column(VARCHAR(100))
    fielder = Column(VARCHAR(100))

class matches(Base):
    __tablename__ = 'matches_data'
    id = Column(Integer,primary_key = True)
    season = Column(VARCHAR(25))
    city = Column(VARCHAR(100))
    date = Column(Date)
    match_type = Column(VARCHAR(100))
    player_of_match = Column(VARCHAR(100))
    venue = Column(VARCHAR(200))
    team_one = Column(VARCHAR(100))
    team_two = Column(VARCHAR(100))
    toss_winner = Column(VARCHAR(100))
    toss_decision = Column(VARCHAR(15))
    winner = Column(VARCHAR(100))
    result = Column(VARCHAR(100))
    result_margin = Column(VARCHAR(20))
    target_runs = Column(Integer)
    target_overs = Column(Integer)
    super_over = Column(VARCHAR(50))
    method = Column(VARCHAR(30))
    umpire_one = Column(VARCHAR(100))
    umpire_two = Column(VARCHAR(100))

engine = create_engine("mysql+mysqlconnector://cmadmin:jjynoq61rlgh@localhost:3306/analytics")
Base.metadata.create_all(engine)
Session = sessionmaker(bind = engine)
session = Session()

def data():
    qp = open('/home/chippu/deliveries.csv','r')
    reader = csv.reader(qp)
    count = 0
    for row in reader:
        if count == 0:
            count += 1
            pass
        else:
            obj = ipl(sl_no = count,
                    id = int(row[0]),
                    innings = int(row[1]),
                    batting_team = row[2],
                    bowling_team = row[3],
                    over = int(row[4]),
                    ball_no = int(row[5]),
                    batter = row[6],
                    bowler = row[7],
                    non_striker = row[8],
                    batsman_runs = int(row[9]),
                    extra_runs = int(row[10]),
                    total_runs = int(row[11]),
                    extras_type = row[12],
                    is_wicket = row[13],
                    player_dismissed = row[14],
                    dismissal_kind = row[15],
                    fielder = row[16])
            count += 1
            session.add(obj)
            session.commit()

def setup_data():
    fp = open('/home/chippu/Downloads/ipl_matches.csv')
    csv_reader = csv.reader(fp)
    count = 0
  
    for row in csv_reader:
        if count == 0:
            count += 1
            continue
        if row[14] == 'NA':
            row[14] = 0
        else:
            match_obj = matches(id = int(row[0]),
                                season = row[1],
                                city = row[2],
                                date = row[3],
                                match_type =row[4],
                                player_of_match = row[5],
                                venue = row[6],
                                team_one = row[7],
                                team_two = row[8],
                                toss_winner = row[9],
                                toss_decision = row[10],
                                winner = row[11],
                                result = row[12],
                                result_margin = row[13],
                                target_runs = int(row[14]),
                                target_overs = int(float(row[15])),
                                super_over = row[16],
                                method =row[17],
                                umpire_one = row[18],
                                umpire_two = row[19])
            session.add(match_obj)
            session.commit()
            count += 1


app = FastAPI()
@app.get("/")
def home():
    return 'welcome to analytics'

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#setup_data()
@app.get("/matches_per_season")
def match_per_season():    #1 
    start = 2008 
    games_per_season = {} 
    for i in range(17):
        start_date = f'{str(start)}/01/01'
        end_date = f'{str(start)}/12/31'
        val = session.query(matches).where(matches.date.between(start_date,end_date)).count()
        games_per_season[start] = val
        start += 1
    return games_per_season
@app.get("/runs_per_season")
def runs_per_season(): #2 
    runs_per_game = session.query(func.sum(ipl.total_runs)).group_by(ipl.id).order_by(ipl.id).all()
    games_per_season = match_per_season()
    runs_per_edition = {}
    start = 0
    for k,v in games_per_season.items():
        total_score = 0
        end = start + v
        for i in range(start,end):
            total_score += runs_per_game[i][0]
        runs_per_edition[k] = total_score
        start = end 
    return runs_per_edition

@app.get("/avg_per_game")
def avg_runs_per_game(): #3
    runs_per_edition = runs_per_season()
    games_per_season = match_per_season()
    avg_runs_dict = {}
    for k,v in runs_per_edition.items():
        avg_score = int(runs_per_edition[k]/ games_per_season[k])
        avg_runs_dict[k] = avg_score
    return avg_runs_dict

@app.get("/umpired_most")
def umpired_most():  #4
    umpire_one_count = session.query(matches.umpire_one,func.count()).group_by(matches.umpire_one).order_by(matches.umpire_one).all()
    umpire_two_count = session.query(matches.umpire_two,func.count()).group_by(matches.umpire_two).order_by(matches.umpire_two).all()
    umpire_dict = {}
    max_matches_umpired = 0
    umpire_most_matches = ''
    for item_one in umpire_one_count:
        for item_two in umpire_two_count:
            if item_one[0] == item_two[0]:
                umpired_matches = item_one[1] + item_two[1]
                umpire_dict[item_one[0]] = umpired_matches
                break

    return umpire_dict

#delhi vs pune - abandoned
#rr vs rcb match abandoned
#2017 - srh rcb
@app.get('/toss_winners')
def team_winning_max_tosses(): #5
    max_toss_winner = session.query(matches.toss_winner,func.count()).group_by(matches.toss_winner)
    toss_winners_dict = {}
    for row in max_toss_winner:
        toss_winners_dict[row[0]] = row[1]
    return toss_winners_dict

@app.get('/toss_decision')
def toss_decisions():  #6
    decision = session.query(matches.toss_decision,func.count()).group_by(matches.toss_decision)
    total_matches = 0
    for i in decision:
        total_matches += i[1]
    percentage = {}
    for j in decision:
        per = (j[1]/total_matches) * 100
        per = round(per,2)
        percentage[j[0]] = per
    return percentage


def season_toss_decision():     #7
    yr = 2008 
    season_toss = {}
    for i in range(17):
        start = f'{str(yr)}/01/01'
        end = f'{str(yr)}/12/31'
        rows = session.query(matches.toss_decision,func.count()).where(matches.date.between(start,end)).group_by(matches.toss_decision).all()
        rows_dict = {}
        for i in rows:
            rows_dict[i[0]] = i[1]
        season_toss[yr] = rows_dict
        yr += 1
    return season_toss

@app.get('/win_games_by_toss')
def win_game_by_toss():          #8
    toss_win = session.query(func.count()).where(matches.toss_winner == matches.winner).all()
    no_of_games = session.query(matches).count()
    win_per = round((toss_win[0][0]/no_of_games)*100,2)
    loss_per = 100 - win_per
    win_los_per = {'win':win_per,'loss':loss_per}
    return win_los_per

@app.get('/win_games_by_chasing')
def match_won_by_chasing():     #9
    game_id = session.query(ipl.id,ipl.batting_team).where(ipl.innings == 2).group_by(ipl.id).all()
    games_lost = 0
    games_won = 0
    for data in game_id:
        chasing_team = session.query(matches.winner).where(int(data[0]) == matches.id).all()
        if chasing_team[0][0] == data[1]:
            games_won += 1
        else: 
            games_lost += 1
    
    return {'games won by chasing': games_won,'games lost by chasing': games_lost}

@app.get('/tournament_winners')
def tournament_winners():      #10
    winners = session.query(matches.winner,func.count()).where(matches.match_type == 'final').group_by(matches.winner).all()
    win = {}
    for each in winners:
        win[each[0]] = each[1]
    return win

@app.get('/matches_per_team')
def no_of_matches_per_team():    #11
    team_matches_one = session.query(matches.team_one,func.count()).group_by(matches.team_one).order_by().all()
    team_matches_two = session.query(matches.team_two,func.count()).group_by(matches.team_two).order_by().all()
    games_played = {}
    for i in range(len(team_matches_one)):
        total_games = int(team_matches_one[i][1]) + int(team_matches_two[i][1])
        games_played[team_matches_one[i][0]] = total_games
    return games_played

#where(matches.date.between('2008/01/01','2020/31/12'))
@app.get('/winner_frequency')
def win_frequency():   #12
    winner_freq = session.query(matches.winner,func.count()).group_by(matches.winner).all()
    winner_freq_dict = {}
    for i in winner_freq:
        if i[0] == 'NA':
            continue
        else:
            winner_freq_dict[i[0]] = i[1]
    return winner_freq_dict

@app.get('/win_percent')
def win_percent():  #13
    team_win_per = session.query(matches.winner,func.count()).group_by(matches.winner).all()
    games_played = no_of_matches_per_team()
    percent_winning = {}
    print(no_of_matches_per_team())
    for team_data in team_win_per:
        if team_data[0] == 'NA':
            continue
        else:
            percent = round((int(team_data[1])/games_played[team_data[0]]) * 100,2)
            percent_winning[team_data[0]] = percent
    return percent_winning

def lucky_venue(team_name):    #14
    venues = session.query(matches.winner,matches.venue,func.count()).where(matches.winner == team_name).group_by(matches.venue).all()
    win_venues = {}
    for i in venues:
        win_venues[i[1]] = i[2]
        return win_venues


def team_comparison():    #15
    pass

def percent_calc(games_dict):
    sum_val = 0
    for v in games_dict.values():
        sum_val += v
    
    for keys,vals in games_dict.items():
        games_dict[keys] = round((vals/sum_val)*100,2) 
    return games_dict


def batsman_analysis(player_name):   #16
    dismissal = session.query(ipl.dismissal_kind,func.count()).where(ipl.player_dismissed == player_name).group_by(ipl.dismissal_kind).all()
    wicket_type = {}
    for row in dismissal:
        wicket_type[row[0]] = row[1]
    
    
    runs_contribution = session.query(ipl.batsman_runs,func.sum(ipl.batsman_runs)).join(matches,ipl.id == matches.id).where(ipl.batter == player_name).group_by(ipl.batsman_runs).all()
    runs_analysis = {}
    for score in runs_contribution:
        runs_analysis[score[0]] = int(score[1])
    
    #print(runs_analysis)
    return percent_calc(wicket_type),percent_calc(runs_analysis)


def innings_wise(): #17
    pass


def two_hundred_runs(): #18 #200+ runs scored by batting team #19
    join_data = (session.query(matches.target_runs,ipl.batting_team,ipl.bowling_team,matches.winner)).join(ipl,ipl.id == matches.id).where(and_(ipl.innings == 1,(matches.target_runs -1) >= 200)).group_by(ipl.id,ipl.batting_team).all()
    two_hundred_plus = {}
    conceded_teams = {}
    for row in join_data:
        if row[3] != row[2] :
            if row[2] not in conceded_teams.keys():
                conceded_teams[row[2]] = 1
            else:
                conceded_teams[row[2]] = conceded_teams[row[2]] + 1
        if row[1] not in two_hundred_plus.keys():
            two_hundred_plus[row[1]] = 1
        else:
            two_hundred_plus[row[1]] = two_hundred_plus[row[1]] + 1
    
    return two_hundred_plus,conceded_teams

def highest_runs_scored(): #20
    highest_run = session.query(func.max(matches.target_runs)).all()
    return highest_run[0][0]

def max_result_margin(): #21
    result_margin = session.query(matches.team_one,matches.team_two,matches.result_margin,matches.date).all()
    max_margin = 0
    margin_data = {}
    for row in result_margin:
        if row[2] == "NA":
            continue
        else:
            if int(row[2]) > max_margin:
                max_margin = int(row[2])
                margin_data['team one'] = row[0]
                margin_data['team two'] = row[1]
                margin_data['match date '] = row[3]
                margin_data['margin'] = int(row[2])
    
    return margin_data

def balls_played_by_batsman(): #22
    balls_played = (session.query(ipl.batter,func.count(ipl.batter))).group_by(ipl.batter).where(and_(ipl.extras_type != 'wides')).order_by(desc(func.count(ipl.batter))).all()
    balls_played_dict = {}
    for i in balls_played:
        balls_played_dict[i[0]] = i[1]
   
    return balls_played_dict
        
def top_runs_scorers(): #23
    top_runs_scored = session.query(ipl.batter,func.sum(ipl.batsman_runs)).group_by(ipl.batter).order_by(func.sum(ipl.batsman_runs)).all()
    super_over_id = (session.query(matches.id,ipl.sl_no)).join(matches,matches.id == ipl.id).where(and_(matches.super_over == 'Y',ipl.over == 19,ipl.ball_no == 5)).all()
    super_over_runs = {}
    for rows in super_over_id:
        batsman_runs_super_over =  session.query(ipl.batter,ipl.batsman_runs).where(and_(ipl.over == 0,ipl.id == rows[0],ipl.sl_no > rows[1])).group_by(ipl.batter).all()
        for inner_row in batsman_runs_super_over:
            if inner_row[0] in super_over_runs.keys():
                super_over_runs[inner_row[0]] = super_over_runs[inner_row[0]] + inner_row[1]
            else:
                super_over_runs[inner_row[0]] = inner_row[1]
    batter_data_dict = {}
    for batters_data in top_runs_scored:
        if batters_data[0] in super_over_runs.keys():
            batter_data_dict[batters_data[0]] = batters_data[1] - super_over_runs[batters_data[0]]
        else:
            batter_data_dict[batters_data[0]] = batters_data[1] 
    return batter_data_dict

def most_no_fours(): #24
    fours = session.query(ipl.batter,func.count(ipl.batsman_runs)).where(ipl.batsman_runs == 4).group_by(ipl.batter).order_by(desc(func.count(ipl.batsman_runs))).all()
    fours_dict = {}
    count = 0
    for row in fours:
        fours_dict[row[0]] = row[1]
        count += 1
        if count == 16:
            return fours_dict

def most_no_sixes(): #25
    sixes = session.query(ipl.batter,func.count(ipl.batsman_runs)).where(ipl.batsman_runs == 6).group_by(ipl.batter).order_by(desc(func.count(ipl.batsman_runs))).all()
    sixes_dict = {}
    count = 0
    for row in sixes:
        sixes_dict[row[0]] = row[1]
        count += 1
        if count == 16:
            return sixes_dict

def strike_rate(): #26
    #runs / balls faced *100
    balls_faced = balls_played_by_batsman()
    runs_scored = top_runs_scorers()
    strike_rate_dict = {}
    for player in balls_faced.keys():
        if balls_faced[player] >= 100:
            strike_rate_dict[player] = round((runs_scored[player] / balls_faced[player]) * 100,3)

    sorted_strike_rate = dict(sorted(strike_rate_dict.items(), key = itemgetter(1), reverse=True))
    return sorted_strike_rate
def all_strike_rates():
    #runs / balls faced *100
    balls_faced = balls_played_by_batsman()
    runs_scored = top_runs_scorers()
    strike_rate_dict = {}
    for player in balls_faced.keys():
         strike_rate_dict[player] = round((runs_scored[player] / balls_faced[player]) * 100,3)

    sorted_strike_rate = dict(sorted(strike_rate_dict.items(), key = itemgetter(1), reverse=True))
    return sorted_strike_rate

def wicket_takers(): #27
    bowler_stats = session.query(ipl.bowler,ipl.dismissal_kind,ipl.fielder).where(ipl.is_wicket == 1).all()
    wickets = {}
    for row in bowler_stats:
        if row[1] != 'run out':
            if row[0] not in wickets.keys():
                wickets[row[0]] = 1
            else:
                wickets[row[0]] = wickets[row[0]] + 1
    wickets = dict(sorted(wickets.items(), key = itemgetter(1), reverse=True))
    return wickets

def stadium_games(): #28
    stadium_stats = session.query(matches.venue,func.count(matches.venue)).group_by(matches.venue).all()
    stadium_dict = {}
    for row in stadium_stats:
        stadium_dict[row[0]] = row[1]
    return stadium_dict

def man_of_match(): #29
    players = session.query(matches.player_of_match,func.count(matches.player_of_match)).group_by(matches.player_of_match).all()
    mom_dict = {}
    for row in players:
        mom_dict[row[0]] = row[1]
    
    return mom_dict

def all_ipl_players():
    all_ipl_players = session.query(ipl.batter,ipl.bowler).group_by(ipl.batter,ipl.bowler).all()
    player_list = []
    for athlete in all_ipl_players:
        if (athlete[0] not in player_list):
            player_list.append(athlete[0])
        
        elif (athlete[1] not in player_list):
            player_list.append(athlete[1])

    return player_list

@app.get('/fours_per_season')
def number_of_fours_per_season(): #30
    no_fours = (session.query(ipl.batsman_runs,matches.season,func.count())).join(matches,matches.id == ipl.id).where(ipl.batsman_runs == 4).group_by(matches.season).all()
    fours_dict = {}
    for row in no_fours:
        fours_dict[row[1]] = row[2]
    return fours_dict

def number_of_sixes_per_season(): #31
    no_sixes = (session.query(ipl.batsman_runs,matches.season,func.count())).join(matches,matches.id == ipl.id).where(ipl.batsman_runs == 6).group_by(matches.season).all()
    sixes_dict = {}
    for row in no_sixes:
        sixes_dict[row[1]] = row[2]
    return sixes_dict

def boundary_runs_per_season(): #32
    fours_dict = number_of_fours_per_season()
    sixes_dict = number_of_sixes_per_season()
    boundary_dict = {}
    for season in fours_dict:
        boundary_dict[season] = (fours_dict[season]*4) + (sixes_dict[season]*6)
    
    return boundary_dict


def contribution_boundaries():#33
    total_runs_per_season = runs_per_season()
    boundary_runs = boundary_runs_per_season()
    contribution_dict = {}
    for yr in total_runs_per_season:
        contribution_dict[yr] = round((boundary_runs[str(yr)] / total_runs_per_season[yr]) *100,2)

    return contribution_dict

def runs_in_powerplay(): #34
    powerplay_runs = (session.query(ipl.batting_team,func.sum(ipl.total_runs))).join(matches,matches.id == ipl.id).where(and_(ipl.over <= 5)).group_by(ipl.batting_team).all()
    power_play_dict = {}
    for row in powerplay_runs:
        power_play_dict[row[0]] = row[1]
    return power_play_dict

def death_over_runs(): #35
    end_over_runs = (session.query(ipl.batting_team,func.sum(ipl.total_runs))).where(ipl.over >= 16).group_by(ipl.batting_team).all()
    end_over_dict = {}
    for row in end_over_runs:
        end_over_dict[row[0]] = row[1]
    return end_over_dict

def power_play_run_rate(): #36
    pass


class Players:
    def __init__(self,name,age):
        self.name = name
        self.age = age
        
    @app.get('/vkclass')
    def class_dict():
        return virat.__dict__

virat = Players('V Kohli',34)
