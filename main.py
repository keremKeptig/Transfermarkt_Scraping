import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pyodbc

def find_player(url):

    global player_names
    global nationality
    global date_of_birth
    global player_market_values
    global player_id
    global player_links
    global player_team_id
    global call
    count_id = 0

    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    tree = requests.get(url, headers=headers)
    soup = BeautifulSoup(tree.content, 'html.parser')

    # All Player Details
    panel = soup.find(id='yw1')
    if soup.find("th",{"id":"yw1_c4"}) is not None:
        current_club = soup.find("th", {"id": "yw1_c4"})


    details_info = panel.find_all("td",{"class":"zentriert"})
    counter = 0
    for info in details_info:
        # it should be skipped because of useless information
        if counter == 3:
            if current_club.text == 'Current club':
                counter = 0
                continue
        if counter == 3:
            counter = 0
        if counter == 1:
            if info.text == '':
                date_of_birth.append('Unknown')
            else:
                date_of_birth.append(info.text)
        elif counter == 2:
            if info.text != '':
                nationality.append('Unknown')
            else:
                nationality.append(info.find('img')['title'])
        counter += 1


    # Player Name, Player id and player links
    name_class = panel.find_all("span", {"class": "hide-for-small"})
    for name in name_class:
        if name.find('a')['title'] != '':
            if name.text == '':
                continue
            else:
                player_names.append(name.text)
        if name.find('a')['href'] != '':
            player_id.append(name.find('a')['href'].split('/')[4])
            player_links.append('https://www.transfermarkt.co.uk' + name.find('a')['href'])
        count_id += 1

    # Market Value
    values = soup.find_all("td", {"class": "rechts hauptlink"})
    for marketValue in values:
        player_market_values.append(marketValue.text.replace('€', '').replace('£', ''))
    # To assign correct team id to players
    for i in range(0, count_id):
        player_team_id.append(team_id[call])
    # To find correct team id
    call += 1


def player_details(url):

    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    tree = requests.get(url, headers=headers)
    soup = BeautifulSoup(tree.content, 'html.parser')

    global history_call
    global season
    global date
    global prev_club
    global next_club
    global market_value_history
    global testimonial
    count = 0
    counter_id = 0
    history_information = []
    header = soup.find("ul", {"class": "data-header__items"})

    #transfer history
    # Table
    history_box = soup.find_all("div", {"class": "box viewport-tracking"})[1]

    # Rows Info
    row_box = history_box.find_all("div", {"class": "tm-player-transfer-history-grid"})

    for information in row_box:
        new_list = []
        if information == row_box[len(row_box) - 1]:
            break
        elif information == row_box[0]:
            continue
        else:
            new_list = information.text.splitlines()
            # Season
            history_information.append(new_list[2].strip())
            # Date
            history_information.append(new_list[4].strip())
            # Previous Club
            history_information.append(new_list[11].strip())
            # Next Club
            history_information.append(new_list[19].strip())
            # Money, market value - Testimonial
            history_information.append(new_list[22].strip().replace('€', ''))
            history_information.append(new_list[24].strip().replace('€', ''))
            # counter for id
            counter_id += 1

    for information in history_information:

        if count == 0:
            season.append(information)
        elif count == 1:
            date.append(information)
        elif count == 2:
            prev_club.append(information)
        elif count == 3:
            next_club.append(information)
        elif count == 4:
            market_value_history.append(information)
        elif count == 5:
            testimonial.append(information)

        if count == 5:
            count = 0
        else:
            count += 1
    for i in range(counter_id):
        history_player_id.append(url.split('/')[6])
    history_call += 1


def leagues_information(url):
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    # Process League Table
    tree = requests.get(url, headers=headers)
    soup = BeautifulSoup(tree.content, 'html.parser')

    # Teams of the League
    league_links, league_name, country, clubs, player, average_age, foreigners, total_value = ([] for i in range(8))

    links = soup.find_all("table", {"class": "inline-table"})
    # get league links
    global total_league_links
    # get links and name of the leagues
    for link in links:
        if link.find('a')['href'] != '':
            league_name.append(link.text.strip())
            get_link = 'https://www.transfermarkt.co.uk' + link.find_all('a')[1]['href']
            total_league_links.append(get_link)
            league_links.append(get_link)

    # get country
    information = soup.find_all("td", {"class": "zentriert"})
    count = 0
    flag = 0
    for info in information:
        if count == 0:
            # get country
            country.append(info.find('img')['title'])
        elif info.text.strip() != '':
            if flag == 1:
                # get club name
                clubs.append(info.text.strip())
            elif flag == 2:
                # get player size
                player.append(info.text.strip())
            elif flag == 3:
                # get average age
                average_age.append(info.text.strip())
            elif flag == 4:
                # get foreigners
                foreigners.append(info.text.strip())
        count += 1
        flag += 1
        # to find proper country
        if count == 6:
            count = 0
        if flag == 6:
            flag = 0

    # total value
    values = soup.find_all("td",{"class":"rechts hauptlink"})
    for value in values:
        total_value.append(value.text)

    league_frame = {"League": league_name, "Country": country, "Clubs": clubs, "Player": player,
                 "Average_Age": average_age, "Foreigners": foreigners,
                 "Link": league_links, "Total_Value": total_value}
    # Saved to data frame as csv
    df = pd.DataFrame(league_frame)

    # append data frame to CSV file
    df.to_csv(r"C:\Users\userpc\Desktop\Leagues.csv", mode='a', index=False, header=False)


# first link different from others, so it called first
page = 'https://www.transfermarkt.co.uk/wettbewerbe/europa?ajax=yw1&page=1'
# get all laegue links
total_league_links = []
leagues_information(page)

for i in range(2, 16):
    new_link = page + '&page=' + str(i)
    leagues_information(new_link)

# To identify leagues
count_league_id = 0

for link in total_league_links:

    player_links = []
    # Teams information
    league_id, team_id, name_of_team, squad_size, squad_foreign_players, market_value, total_market_value = ([] for i in range(7))
    # Players information
    player_team_id, player_id, player_names,  date_of_birth, nationality, player_market_values = ([] for i in range(6))
    # History information
    history_player_id, season, date, prev_club, market_value_history, testimonial = ([] for i in range(6))
    call = 0
    history_call = 0
    # history section
    next_club = []

    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    tree = requests.get(link.strip(), headers=headers)
    soup = BeautifulSoup(tree.content, 'html.parser')
    # Teams of the League
    teamLinks = []

    # All Player Details
    panel = soup.find(id='yw1')

    squad_average_age = []

    # Team links
    for club in soup.findAll('td', {'class': 'hauptlink no-border-links'}):
        if club.text != '':
            name_of_team.append(club.find('a')['title'])  # .split('/')[1]
            team_id.append(club.find('a')['href'].split('/')[4])
            teamLinks.append('https://www.transfermarkt.co.uk' + club.find('a')['href'])
    # correct league id for teams
    for i in team_id:
        league_id.append(count_league_id)
    # increment one to assign unique id
    count_league_id += 1
    # squad capacity, age, foreign players
    all_important_info = panel.find_all("td", {"class": "zentriert"})
    flag = 0
    count = 3
    correct_order = 0
    for info in all_important_info:
        # skip useless information
        if flag != 3:
            flag += 1
        else:
            if count < 3:
                if correct_order == 3:
                    correct_order = 0
                # squad size
                if correct_order == 0:
                    squad_size.append(info.text)
                # squad average age
                elif correct_order == 1:
                    squad_average_age.append(info.text)
                # squad foreign players
                else:
                    squad_foreign_players.append(info.text)
                correct_order += 1
                count += 1
            else:
                count = 0
    # market value, total market value
    all_details_club = panel.find_all("td", {"class": "rechts"})
    count = 0
    flag = 0
    if len(all_details_club) == 0:
        for i in range(len(squad_size)):
            market_value.append("There is no Market value")
            total_market_value.append("There is no Total market value")
    else:
        for details in all_details_club:
            # skip useless information
            if flag != 2:
                flag += 1
            else:
                if count == 0:
                    market_value.append(details.text.replace('£', '').replace('€', ''))
                elif count == 1:
                    total_market_value.append(details.text.replace('£', '').replace('€', ''))

                if count == 1:
                    count = -1
                count += 1

    if __name__ == '__main__':

        # Details of Players
        for links in teamLinks:
            find_player(links)
        with ThreadPoolExecutor(max_workers=3) as p:
            p.map(player_details, player_links)

        # data frame is created using all collected information
        team_list = {"League_id":league_id, "id": team_id, "Team Name": name_of_team, "Squad Size": squad_size, "Average Age": squad_average_age,
             "Foreign Players": squad_foreign_players, "Market Value": market_value,
             "Total Market Value": total_market_value}
        # Saved to data frame as csv
        df = pd.DataFrame(team_list)

        # append data frame to CSV file
        df.to_csv(r"C:\Users\userpc\Desktop\Teams.csv", mode='a', index=False, header=False)


        league_name = link.split('/')[3]
        team_player = {"Team Id": player_team_id, "Player Id": player_id, "Player Name": player_names,
             "Date Of Birth": date_of_birth, "Nationality": nationality, "Player Market Value": player_market_values}
        # Saved to data frame as csv
        df = pd.DataFrame(team_player)

        # append data frame to CSV file
        df.to_csv(r"C:\Users\userpc\Desktop\Team_Players.csv", mode='a', index=False, header=False)

        history_player = {"Id": history_player_id, "Season": season, "Date": date, "Previous Club": prev_club, "Next Club": next_club,
                          "Market Value":market_value_history, "Testimonial": testimonial}
        # Saved to data frame as csv
        df = pd.DataFrame(history_player)

        # append data frame to CSV file
        df.to_csv(r"C:\Users\userpc\Desktop\transfer_history.csv", mode='a', index=False, header=False)


# connect to sql database
conn_str = ("Driver={SQL Server Native Client 11.0};"
                      "Server=tcp:myserver.database.windows.net;"
                      "Database=mydb;"
                      "Trusted_Connection=yes;")
cnxn = pyodbc.connect(conn_str)
df = pd.read_csv(r"C:\Users\userpc\Desktop\Leagues.csv", on_bad_lines='skip')
df = df.fillna(value=0)

cursor = cnxn.cursor()
print(cursor)
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO dbo.Leagues(league_id,league_name,country,clubs,total_player,average_age,foreigners,league_link,total_value) values(?,?,?,?,?,?,?,?,?)", row.league_id,row.league_name,row.country,row.clubs,row.total_player,row.average_age,row.foreigners,row.league_link,row.total_value)
cnxn.commit()

df = pd.read_csv(r"C:\Users\userpc\Desktop\Teams.csv", on_bad_lines='skip')
df = df.fillna(value=0)
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO dbo.Teams(Id,Team_Id,Squad_Size,Average_Age,Foreign_Players,Market_Value,Total_Market_Value) values(?,?,?,?,?,?,?)", row.Id,row.Team_Id,row.Squad_Size,row.Average_Age,row.Foreign_Players,row.Market_Value,row.Total_Market_Value)
cnxn.commit()

df = pd.read_csv(r"C:\Users\userpc\Desktop\Team_Players.csv", on_bad_lines='skip')
df = df.fillna(value=0)
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO dbo.Players(Team_Id,Team_Name,Player_Id,Player_Name,Date_Of_Birth,Nationality,Player_Market_Value) values(?,?,?,?,?,?,?)", row.Team_Id,row.Team_Name,row.Player_Id,row.Player_Name,row.Date_Of_Birth,row.Nationality,row.Player_Market_Value)
cnxn.commit()

df = pd.read_csv(r"C:\Users\userpc\Desktop\transfer_history.csv", on_bad_lines='skip')
df = df.fillna(value=0)
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO dbo.Transfer_history(Player_Id,Season,Date,Left,Joined,Market_Value,Fee) values(?,?,?,?,?,?,?)", row.Player_Id,row.Season,row.Date,row.Left,row.Joined,row.Market_Value,row.Fee)
cnxn.commit()
