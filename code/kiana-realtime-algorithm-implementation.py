import time
import itertools
import pandas as pd
import math

####
# The following has taken the code used to generate results for our real time algorithm and breaks it down into
# a collection of functions that can be used or altered to fit Kiana's systems. The first function is the main function
# which uses all functions to do the tasks necessary to keep track of device matches in real time given a stream of
# data. This stream of live data that Kiana actually uses is not one we had access to during our project. Because of
# this, we were forced to simulate this stream using a static array sorted by time. The use of variables base_time,
# this_time, and i is explicitly for that purpose and would be changed if this code was ever properly integrated in
# Kiana systems. Currently the data stream is represented as the variable "data_stream" but is treated as a pandas
# dataframe in this code. This data stream is meant to be a stream of each data point collected in a single day.
#
# Algorithm: Data streams in by row in the form it was given to us for this project. A dictionary has table is
# constructed to store the data every "time_step" (timedelta variable) number of seconds. As a data point is added,
# it is hashed based on its location within a given "radius" (int variable) as well as the floor it was recorded on.
# this gives us a hash table where every entry that has a list longer than one represents a match for that time step.
# A match in this case, means two MacId's were caught within "radius" of each other in the given two second interval.
# After each time step, the data is then used to update the data structure keeping track of all matched pairs and their
# history. The variable name for this task is "match_tracker", it is a dictionary where the keys are the MacId's of a
# given pair of matched devices, sorted in lexicographic order. We also track the positions and frequency of being
# detected of every MacId in the dictionary "id_tracker" which uses the MacId strings as keys. The purpose of these
# data structures is to be searchable at any time in the case of a stolen person or device, or to be used to update
# population statistics based on which devices are being double counted. In using this algorithm you gain access to
# information on which MacId's consistently show up together and their individual paths. We have developed metrics as
# well to allow us to draw conclusions on which devices we consider to be from the same person.
####


def main(data_stream):
    # these two parameters can be altered if Kiana is able to perform experiments and conclude they should be changed.
    time_step = '2 seconds'
    radius = 0.0000359  # about 4 meters

    # keeps track of each match made throughout the day
    match_tracker = dict()

    # keeps track of how each Id throughout the day
    id_tracker = dict()

    # these variables are only used in simulating the data stream
    base_time = pd.to_datetime(data_stream['datetime'][0])
    this_time = pd.to_datetime(data_stream['datetime'][0])
    while data_stream:
        current_buffer = dict()

        # this variable is only used in simulating the data stream
        i = 0

        while this_time < base_time + pd.Timedelta(time_step):
            cur_id = data_stream['MacId'][i]
            lat = data_stream['lat'][i]
            lng = data_stream['lng'][i]
            floor = data_stream['floor'][i]
            time_caught = data_stream['datetime'][i]

            add_id_to_grid(current_buffer, cur_id, lat, lng, floor, radius)
            update_id_tracker(id_tracker, cur_id, lat, lng, time_caught)

            # more data stream simulation
            i += 1
            this_time = data_stream['datetime'][i]

        update_match_tracker(current_buffer, match_tracker, base_time)

        # more data stream simulation
        base_time = this_time

    print_results(match_tracker)


##
# Use: this function is meant to take the current time buffer of data and add it to the ongoing data structure that
# stores match history.
#
# input:
# buffer - the dictionary containing data on the past time step and what matches were found
# tracker - the dictionary being used to keep track of match history
# match_base_time - Each time step has a start time and a stop time, This is the start time, and is the time we use to
# refer to all data caught in this time step
#
# output:
# There is no explicit output, but by the end of this function the match_tracker should be up to date.
# The match_tracker key was explained in the opening statements, the value for each key is a list with three dimensions.
# The first dimension is an int representing how many times the match has been made, the second is as list of each
# time step base_time each match was made (should be as long as the int in the first dimension), the third dimension
# is the matched MacID's themselves
##
def update_match_tracker(buffer, tracker, match_base_time):
    # update probabilities with current buffer results
    for key in buffer:
        # remove duplicates
        matched_devices = remove_dup(buffer[key])
        # match made if hash table has multiple entries in one spot
        if (len(matched_devices) > 1) and (len(matched_devices) < 7):
            matched_devices = sorted(matched_devices)  # always hash sorted list to find duplicates
            # consider all subsets of size 2
            all_subsets = list(itertools.combinations(range(len(matched_devices)), 2))
            for subset in all_subsets:
                these_devices = [matched_devices[subset[0]], matched_devices[subset[1]]]

                match_hash = hash(tuple(these_devices))

                # update or initialize this match
                if match_hash in tracker:
                    tracker[match_hash][0] += 1
                    tracker[match_hash][1].append(match_base_time)
                else:
                    tracker[match_hash] = [1, [match_base_time], these_devices]


##
# Use: This function is meant to take in data on a current data point and the current time buffer data and update it
# based on the new data point. The update is done by adding the point to the hash dictionary.
#
# input:
# buffer - the has dictionary for the current time buffer
# this_id, lat, lng, floor - This is the MacId, latitude, longitude, and floor location data for the current data point
# r - the radius being used for the hash
#
# output: no explicit output, the current buffer should now include data on the passed data point
##
def add_id_to_grid(buffer, this_id, lat, lng, floor, r):

    # hash each macid based on location
    v1 = math.floor(lng / r)
    v2 = math.floor(lat / r)
    v3 = floor
    key = hash((v1, v2, v3))
    if key in buffer:
        buffer[key].append(this_id)
    else:
        buffer[key] = [this_id]


##
# Use: this function updates the passed id_tracker with information on the passed data point.
#
# input:
# tracker - the ID tracker
# this_id, lat, lng - This is the MacId, latitude, longitude data for the current data point
# time_ - the time the datapoint was caught
#
# output: no output, the id_tracker should be updated when this function finishes
# id_tracker structure: dictionary who's keys are the MacId's of each device. The values are each lists of length three
# The first entry is the number of times this Id was caught, the second is as list of (lat, lng) tuples for each time
# the id was caught, the third is a list of times the id was caught at, should be the same length as the second entry.
# and the length should be equal to the first entry value.
##
def update_id_tracker(tracker, this_id, lat, lng, time_):
    # update mac tracker
    if this_id in tracker:
        tracker[this_id][0] += 1
        tracker[this_id][1].append((lat, lng))
        tracker[this_id][2].append(time_)
    else:
        tracker[this_id] = []
        tracker[this_id].append(1)
        tracker[this_id].append([(lat, lng)])
        tracker[this_id].append(time_)


##
# Use: This function computes the time metric based on two passed MacId's
#
# input:
# id1, id2 - the two MacId's as strings you wish to compute the time metric for
# id_tracker, match_tracker - the data structures with history's of each matched pair and MacId
#
# output: score, -1 if score could not be computed
# Score = 2 * the amount of time matched/ (time spent in Museum for id1 + time spend in Museum for id2)
##
def match_time_metric(id1, id2, id_tracker, match_tracker):
    dev1_tot_time = (id_tracker[id1][2][-1].hour - id_tracker[id1][2][0].hour) * 60 + id_tracker[id1][2][-1].minute - \
                    id_tracker[id1][2][0].minute
    dev2_tot_time = (id_tracker[id2][2][-1].hour - id_tracker[id2][2][0].hour) * 60 + id_tracker[id2][2][-1].minute - \
                    id_tracker[id2][2][0].minute

    devs = sorted([id1, id2])
    time_matched = (match_tracker[hash(tuple(devs))][2][-1].hour - match_tracker[hash(tuple(devs))][2][0].hour) * 60 + \
                   match_tracker[hash(tuple(devs))][2][-1].minute - match_tracker[hash(tuple(devs))][2][0].minute
    try:
        score = (2 * time_matched) / (dev1_tot_time + dev2_tot_time)
    except ZeroDivisionError:
        score = -1

    return score


##
# Use: This metric computes the match_time_metric and the adjusts it based on how often matches were made
#
# input:
# id1, id2 - the two MacId's as strings you wish to compute the time metric for
# id_tracker, match_tracker - the data structures with history's of each matched pair and MacId
# output: score, -1 if the score could not be computed
#
# Score = time_metric_score * c
# c = frequency of match/ (frequency of id1 being caught + frequency of id2 being caught)
##
def adj_time_metric(id1, id2, id_tracker, match_tracker):
    score = match_time_metric(id1, id2, id_tracker, match_tracker)
    devs = sorted([id1, id2])

    score = score * match_tracker[hash(tuple(devs))][0] / (id_tracker[id1][0] + id_tracker[id2][0])

    if score < 0:
        return -1
    else:
        return score


# this function simply takes a list of strings and returns a list of string without duplicates
def remove_dup(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list


# this function is meant to print results as they are seen in the Jupyter notebook for the real time algorithm
# it takes the match tracker and id tracker as inputs and does not return anything
def print_results(m_tracker):

    df = pd.DataFrame()
    counts = []
    ids = []
    adj_metrics = []
    time_metrics = []
    for key, match_list in m_tracker.items():
        counts.append(match_list[1])
        ids.append(match_list[3])

        time_metrics.append(match_time_metric(match_list[3][0], match_list[3][1]))
        adj_metrics.append(adj_time_metric(match_list[3][0], match_list[3][1]))

    df['counts'] = counts
    df['time metric'] = time_metrics
    df['adj metric'] = adj_metrics
    df['MacId'] = ids

    df = df.sort_values(by=['counts', 'time metric', 'adj metric'], ascending=False).reset_index(drop=True)

    for i in range(5000):
        if df.counts[i] > 100:
            print(df.MacId[i], 'time metric:', df['time metric'][i], 'adjusted metric:', df['adj metric'][i])

