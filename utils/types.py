from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.sql.types import BooleanType
from enum import StrEnum


def get_dataset_sample_structure_schema():
    return StructType() \
        .add("#", IntegerType(), True) \
        .add("app_id", IntegerType(), True) \
        .add("app_name", StringType(), True) \
        .add("review_id", IntegerType(), True) \
        .add("language", StringType(), True) \
        .add("review", StringType(), True) \
        .add("timestamp_created", IntegerType(), True) \
        .add("timestamp_updated", IntegerType(), True) \
        .add("recommended", BooleanType(), True) \
        .add("votes_helpful", IntegerType(), True) \
        .add("votes_funny", IntegerType(), True) \
        .add("weighted_vote_score", FloatType(), True) \
        .add("comment_count", IntegerType(), True) \
        .add("steam_purchase", BooleanType(), True) \
        .add("received_for_free", BooleanType(), True) \
        .add("written_during_early_access", BooleanType(), True) \
        .add("author_steamid", IntegerType(), True) \
        .add("author_num_games_owned", IntegerType(), True) \
        .add("author_num_reviews", IntegerType(), True) \
        .add("author_playtime_forever", FloatType(), True) \
        .add("author_playtime_last_two_weeks", FloatType(), True) \
        .add("author_playtime_at_review", FloatType(), True) \
        .add("author_last_played", IntegerType(), True)


def get_original_dataset_structure_schema():
    return StructType() \
        .add("#", IntegerType(), True) \
        .add("app_id", IntegerType(), True) \
        .add("app_name", StringType(), True) \
        .add("review_id", IntegerType(), True) \
        .add("language", StringType(), True) \
        .add("review", StringType(), True) \
        .add("timestamp_created", IntegerType(), True) \
        .add("timestamp_updated", IntegerType(), True) \
        .add("recommended", BooleanType(), True) \
        .add("votes_helpful", IntegerType(), True) \
        .add("votes_funny", IntegerType(), True) \
        .add("weighted_vote_score", FloatType(), True) \
        .add("comment_count", IntegerType(), True) \
        .add("steam_purchase", BooleanType(), True) \
        .add("received_for_free", BooleanType(), True) \
        .add("written_during_early_access", BooleanType(), True) \
        .add("author.steamid", IntegerType(), True) \
        .add("author.num_games_owned", IntegerType(), True) \
        .add("author.num_reviews", IntegerType(), True) \
        .add("author.playtime_forever", FloatType(), True) \
        .add("author.playtime_last_two_weeks", FloatType(), True) \
        .add("author.playtime_at_review", FloatType(), True) \
        .add("author.last_played", IntegerType(), True)


class SampleColumn(StrEnum):
    INDEX = "#"
    APP_ID = "app_id"
    APP_NAME = "app_name"
    REVIEW_ID = "review_id"
    LANGUAGE = "language"
    REVIEW = "review"
    TIMESTAMP_CREATED = "timestamp_created"
    TIMESTAMP_UPDATED = "timestamp_updated"
    RECOMMENDED = "recommended"
    VOTES_HELPFUL = "votes_helpful"
    VOTES_FUNNY = "votes_funny"
    WEIGHTED_VOTE_SCORE = "weighted_vote_score"
    COMMENT_COUNT = "comment_count"
    STEAM_PURCHASE = "steam_purchase"
    RECEIVED_FOR_FREE = "received_for_free"
    WRITTEN_DURING_EARLY_ACCESS = "written_during_early_access"
    AUTHOR_STEAMID = "author_steamid"
    AUTHOR_NUM_GAMES_OWNED = "author_num_games_owned"
    AUTHOR_NUM_REVIEWS = "author_num_reviews"
    AUTHOR_PLAYTIME_FOREVER = "author_playtime_forever"
    AUTHOR_PLAYTIME_LAST_TWO_WEEKS = "author_playtime_last_two_weeks"
    AUTHOR_PLAYTIME_AT_REVIEW = "author_playtime_at_review"
    AUTHOR_LAST_PLAYED = "author_last_played"


class OriginalColumn(StrEnum):
    """WARNING: this is not compatible with all spark modules"""
    INDEX = "#"
    APP_ID = "app_id"
    APP_NAME = "app_name"
    REVIEW_ID = "review_id"
    LANGUAGE = "language"
    REVIEW = "review"
    TIMESTAMP_CREATED = "timestamp_created"
    TIMESTAMP_UPDATED = "timestamp_updated"
    RECOMMENDED = "recommended"
    VOTES_HELPFUL = "votes_helpful"
    VOTES_FUNNY = "votes_funny"
    WEIGHTED_VOTE_SCORE = "weighted_vote_score"
    COMMENT_COUNT = "comment_count"
    STEAM_PURCHASE = "steam_purchase"
    RECEIVED_FOR_FREE = "received_for_free"
    WRITTEN_DURING_EARLY_ACCESS = "written_during_early_access"
    AUTHOR_STEAMID = "author.steamid"
    AUTHOR_NUM_GAMES_OWNED = "author.num_games_owned"
    AUTHOR_NUM_REVIEWS = "author.num_reviews"
    AUTHOR_PLAYTIME_FOREVER = "author.playtime_forever"
    AUTHOR_PLAYTIME_LAST_TWO_WEEKS = "author.playtime_last_two_weeks"
    AUTHOR_PLAYTIME_AT_REVIEW = "author.playtime_at_review"
    AUTHOR_LAST_PLAYED = "author.last_played"
