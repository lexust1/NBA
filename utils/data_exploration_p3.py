def compare_players(df1, df2, names):
    """Compares data for every table from two sources.

    The function prints a lists of players data from two sources. This
    data can be pasted and saved as a csv-file. It helps to find the
    same players.
    """
    player_list = []
    manual_list = []
    cnt_correct_rows = 0
    cnt = 0
    for name in names:#[1:40]:
        rows_in_df1 = df1[df1["name"] == name].shape[0]
        rows_in_df2 = df2[df2["name"] == name].shape[0]
        print(name)
        for row1 in range(rows_in_df1):
            for row2 in range(rows_in_df2):
                res = df1[df1["name"] == name].iloc[row1].tolist() + df2[df2["name"] == name].iloc[row2].tolist()
                print(res)
                if df1[df1["name"] == name].iloc[row1, 3] - df2[df2["name"] == name].iloc[row2, 3] == 0:
                    res = df1[df1["name"] == name].iloc[row1].tolist() + df2[df2["name"] == name].iloc[row2].tolist()
                    player_list.append(res)
                    cnt_correct_rows += 1
                    cnt += 1
                else:
                    res = df1[df1["name"] == name].iloc[row1].tolist() + df2[df2["name"] == name].iloc[row2].tolist()
                    manual_list.append(res)
