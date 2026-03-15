# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # AI Review Summarizer
# MAGIC
# MAGIC This notebook reads customer reviews and uses Claude Sonnet 4.6 to generate
# MAGIC AI-powered summaries and sentiment scores.

# COMMAND ----------

from pyspark.sql import functions as F

# Read customer reviews (limit to 10 for demo speed)
reviews_df = spark.table("samples.bakehouse.media_customer_reviews").limit(10)

display(reviews_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate AI Summaries and Sentiment Scores

# COMMAND ----------

summarized_df = reviews_df.withColumn(
    "ai_summary",
    F.expr("""
        ai_query(
            'databricks-claude-sonnet-4-6',
            CONCAT('Summarize this customer review in one sentence: ', review)
        )
    """)
).withColumn(
    "sentiment_score",
    F.expr("""
        ai_query(
            'databricks-claude-sonnet-4-6',
            CONCAT('Classify the sentiment of this review as exactly one word - Positive, Negative, or Neutral: ', review)
        )
    """)
)

display(summarized_df.select("review", "ai_summary", "sentiment_score"))
