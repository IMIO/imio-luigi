# -*- coding: utf-8 -*-
from difflib import SequenceMatcher
from statistics import mean

SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITH_WARNING = 0.9
SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITHOUT_WARNING = 1
SIMILARITY_THRESHOLD_WITH_TERM_WITH_WARNING = 0.9
SIMILARITY_THRESHOLD_WITH_TERM_WITHOUT_WARNING = 1


def calculate_similarity(str1, str2):
    seq_matcher = SequenceMatcher(None, str1, str2)
    return seq_matcher.ratio()


def find_most_similar_result(results):
    num_results = len(results)

    similarity_list = []

    for i in range(num_results):
        for j in range(i + 1, num_results):
            similarity_list.append(calculate_similarity(results[i]["name"], results[j]["name"]))

    if all(filter(lambda a: a > SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITHOUT_WARNING, similarity_list)):
        return True, None
    elif all(filter(lambda a: a > SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITH_WARNING, similarity_list)):
        return True, f"Attention, la ressemblance entre les noms de rue n'est de que {mean(similarity_list)*100}%"
    else:
        return False, f"Pas assez d'assurance qu'il y a de la ressemblance entre les noms de rue trouvé."


def find_most_similar_term(term, results):
    highest_similarity = 0
    most_similar_result = None

    for result in results:
        similarity_ratio = calculate_similarity(term, result["name"])

        if similarity_ratio > highest_similarity:
            highest_similarity = similarity_ratio
            most_similar_result = result

    return most_similar_result, highest_similarity


def find_address_similarity(results, term=None):

    similarity_between_results, error = find_most_similar_result(results)

    if similarity_between_results:
        return results[0], error

    if term:
        most_similar_result, highest_similarity = find_most_similar_term(term, results)
        if highest_similarity > SIMILARITY_THRESHOLD_WITH_TERM_WITHOUT_WARNING:
            return most_similar_result, None
        elif highest_similarity > SIMILARITY_THRESHOLD_WITH_TERM_WITH_WARNING:
            return most_similar_result, f"Attention, la ressemblance entre les noms de rue n'est de que {(highest_similarity)*100}%"

    return None, None



def find_address_match(values, expected_value):
    exact_match = []
    for item in values:
        name = item["name"].split("(")[0].strip()
        if name.upper() == expected_value.strip().upper():
            exact_match.append(item)
    if len(exact_match) == 1:
        return exact_match[0]
    return []
