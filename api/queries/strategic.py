"""
strategic.py — Tier 2: What People Talk About (deep topic intelligence)

Provides: topicBubbles, trendLines, heatmap, questionCategories,
questionBriefCandidates, lifecycleStages
"""
from __future__ import annotations
from api.db import run_query


def get_topic_bubbles() -> list[dict]:
    """Topic bubble chart: recent topic prominence + reliable short-term growth."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WITH t, cat,
             count(CASE WHEN p.posted_at > datetime() - duration('P14D') THEN 1 END) AS postMentions14d,
             count(CASE WHEN p.posted_at > datetime() - duration('P7D') THEN 1 END) AS posts7d,
             count(CASE WHEN p.posted_at > datetime() - duration('P14D')
                         AND p.posted_at <= datetime() - duration('P7D') THEN 1 END) AS postsPrev7d
        OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
        WITH t, cat, postMentions14d, posts7d, postsPrev7d,
             count(CASE WHEN c.posted_at > datetime() - duration('P14D') THEN 1 END) AS commentMentions14d,
             count(CASE WHEN c.posted_at > datetime() - duration('P7D') THEN 1 END) AS comments7d,
             count(CASE WHEN c.posted_at > datetime() - duration('P14D')
                         AND c.posted_at <= datetime() - duration('P7D') THEN 1 END) AS commentsPrev7d
        OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
        WITH t.name AS name, cat.name AS category,
             postMentions14d, commentMentions14d,
             count(DISTINCT u) AS userInterest,
             coalesce(sum(i.count), 0) AS totalInteractions,
             (posts7d + comments7d) AS mentions7d,
             (postsPrev7d + commentsPrev7d) AS mentionsPrev7d,
             (postMentions14d + commentMentions14d) AS mentionCount14d
        WITH name, category, postMentions14d, commentMentions14d, mentionCount14d,
             userInterest, totalInteractions, mentions7d, mentionsPrev7d
        WHERE mentionCount14d > 0
        RETURN name, category, postMentions14d AS postMentions, commentMentions14d AS commentMentions,
               mentionCount14d AS mentionCount,
               userInterest, totalInteractions,
               mentions7d, mentionsPrev7d,
               (mentions7d + mentionsPrev7d) AS growthSupport,
               CASE
                    WHEN (mentions7d + mentionsPrev7d) < 8 THEN null
                    ELSE round(100.0 * (mentions7d - mentionsPrev7d) / (mentionsPrev7d + 3), 1)
                END AS growth7dPct
        ORDER BY mentionCount DESC
    """)


def get_trend_lines() -> list[dict]:
    """Weekly conversation counts (posts + comments) per topic for the last 8 weeks."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P56D')
            WITH date(p.posted_at).year AS year,
                 date(p.posted_at).week AS week,
                 count(*) AS c
            RETURN year, week, c
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P56D')
            WITH date(c.posted_at).year AS year,
                 date(c.posted_at).week AS week,
                 count(*) AS c
            RETURN year, week, c
        }
        WITH t.name AS topic, year, week, sum(c) AS mentions
        WHERE mentions > 0
        RETURN topic, year, week, mentions AS posts
        ORDER BY topic, year, week
    """)


def get_heatmap() -> list[dict]:
    """Content type × topic matrix (media_type distribution per topic)."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WITH t.name AS topic, coalesce(p.media_type, 'text') AS mediaType,
             count(p) AS count
        RETURN topic, mediaType, count
        ORDER BY topic, count DESC
    """)


def get_question_categories() -> list[dict]:
    """Real question messages by topic with deduplication and response proxy."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P30D')
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            RETURN 'post:' + coalesce(p.uuid, elementId(p)) AS msg_key,
                   coalesce(p.uuid, 'post:' + elementId(p)) AS sample_id,
                   trim(p.text) AS txt,
                   p.posted_at AS ts
            UNION
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P30D')
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            RETURN 'comment:' + coalesce(c.uuid, elementId(c)) AS msg_key,
                   coalesce(c.uuid, 'comment:' + elementId(c)) AS sample_id,
                   trim(c.text) AS txt,
                   c.posted_at AS ts
        }
        WITH t, cat, msg_key, sample_id, txt, ts,
             toLower(replace(replace(replace(replace(txt, '\n', ' '), '\r', ' '), '  ', ' '), '**', '')) AS normalized
        ORDER BY ts DESC
        WITH t, cat, normalized,
             count(DISTINCT msg_key) AS asks_per_form,
             max(ts) AS last_seen,
             head(collect(txt)) AS sample_question,
             head(collect(sample_id)) AS sample_question_id
        ORDER BY asks_per_form DESC, last_seen DESC
        WITH t, cat,
             collect({
                 form: normalized,
                 asks: asks_per_form,
                 sample: sample_question,
                 sample_id: sample_question_id,
                 last_seen: last_seen
             }) AS forms,
             sum(asks_per_form) AS times_asked
        WHERE times_asked > 0

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P30D')
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS sample_id,
                   trim(p.text) AS txt,
                   p.posted_at AS ts
            UNION
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P30D')
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS sample_id,
                   trim(c.text) AS txt,
                   c.posted_at AS ts
        }
        WITH t, cat, forms, times_asked, sample_id, txt, ts
        ORDER BY ts DESC
        WITH t, cat, forms, times_asked,
             head(collect(sample_id)) AS latest_sample_id,
             head(collect(txt)) AS latest_sample

        CALL (t) {
            OPTIONAL MATCH (u:User)-[:EXHIBITS]->(:Intent {name: 'Information Seeking'})
            WHERE EXISTS { MATCH (u)-[:INTERESTED_IN]->(t) }
            OPTIONAL MATCH (:User)-[r:REPLIED_TO_USER]->(u)
            RETURN
                count(DISTINCT u) AS intent_seekers,
                count(DISTINCT CASE
                    WHEN r.last_seen > datetime() - duration('P14D') THEN u
                END) AS responded_intent_seekers
        }

        WITH t, cat, forms, times_asked, latest_sample_id, latest_sample, intent_seekers, responded_intent_seekers

        WITH cat.name AS category,
             t.name AS topic,
             times_asked,
             forms,
             latest_sample_id,
             latest_sample,
             intent_seekers,
             responded_intent_seekers,
             CASE
               WHEN intent_seekers > 0
               THEN toInteger(round(times_asked * (toFloat(responded_intent_seekers) / toFloat(intent_seekers))))
               ELSE 0
             END AS responded_ask_estimate

        RETURN
            category,
            topic,
            times_asked AS seekers,
            CASE
              WHEN responded_ask_estimate > times_asked THEN times_asked
              WHEN responded_ask_estimate < 0 THEN 0
              ELSE responded_ask_estimate
            END AS respondedSeekers,
            CASE
                WHEN times_asked > 0
                THEN round(100.0 * toFloat(CASE
                    WHEN responded_ask_estimate > times_asked THEN times_asked
                    WHEN responded_ask_estimate < 0 THEN 0
                    ELSE responded_ask_estimate
                END) / toFloat(times_asked), 1)
                ELSE 0.0
            END AS coveragePct,
            coalesce(latest_sample, forms[0].sample) AS sampleQuestion,
            coalesce(latest_sample_id, forms[0].sample_id) AS sampleQuestionId
        ORDER BY seekers DESC, category, topic
    """)


def get_question_brief_candidates(
    *,
    days: int = 30,
    limit_topics: int = 14,
    evidence_per_topic: int = 14,
) -> list[dict]:
    """Topic-scoped candidate bundles for AI-generated question briefs."""
    safe_days = max(7, min(int(days), 90))
    safe_limit_topics = max(4, min(int(limit_topics), 40))
    safe_evidence = max(6, min(int(evidence_per_topic), 24))

    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration({days: $days})
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                count(*) AS postSignals,
                count(CASE WHEN p.posted_at > datetime() - duration('P7D') THEN 1 END) AS postSignals7d,
                count(CASE WHEN p.posted_at > datetime() - duration('P14D')
                            AND p.posted_at <= datetime() - duration('P7D') THEN 1 END) AS postSignalsPrev7d,
                collect(DISTINCT coalesce(ch.title, ch.username, '')) AS postChannels
        }

        CALL (t) {
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration({days: $days})
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                count(*) AS commentSignals,
                count(DISTINCT toString(u.telegram_user_id)) AS uniqueUsers,
                count(CASE WHEN c.posted_at > datetime() - duration('P7D') THEN 1 END) AS commentSignals7d,
                count(CASE WHEN c.posted_at > datetime() - duration('P14D')
                            AND c.posted_at <= datetime() - duration('P7D') THEN 1 END) AS commentSignalsPrev7d,
                collect(DISTINCT coalesce(ch.title, ch.username, '')) AS commentChannels
        }

        WITH t, cat,
             postSignals,
             commentSignals,
             uniqueUsers,
             (postSignals + commentSignals) AS signalCount,
             (postSignals7d + commentSignals7d) AS signals7d,
             (postSignalsPrev7d + commentSignalsPrev7d) AS signalsPrev7d,
             [x IN (postChannels + commentChannels)
                WHERE trim(coalesce(toString(x), '')) <> ''
                | toString(x)] AS channelList
        WHERE signalCount > 0

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d,
             reduce(acc = [], item IN channelList |
                CASE WHEN item IN acc THEN acc ELSE acc + item END) AS uniqueChannels
        ORDER BY signalCount DESC
        LIMIT $limit_topics

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration({days: $days})
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                p.posted_at AS ts
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration({days: $days})
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                c.posted_at AS ts
        }

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d, uniqueChannels,
             evidenceId, kind, text, parentText, channel, userId, ts
        ORDER BY ts DESC

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d, uniqueChannels,
             collect({
                id: evidenceId,
                kind: kind,
                text: text,
                parentText: parentText,
                channel: channel,
                userId: userId,
                timestamp: toString(ts)
             })[..$evidence_per_topic] AS evidence,
             max(ts) AS latestTs

        RETURN
            t.name AS topic,
            cat.name AS category,
            signalCount,
            uniqueUsers,
            size(uniqueChannels) AS channelCount,
            signals7d,
            signalsPrev7d,
            toString(latestTs) AS latestAt,
            evidence
        ORDER BY signalCount DESC, latestAt DESC
        """,
        {
            "days": safe_days,
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
        },
    )


def get_lifecycle_stages() -> list[dict]:
    """Topic lifecycle using rolling weekly signals (posts + comments)."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P84D')
            WITH date(p.posted_at).year AS year,
                 date(p.posted_at).week AS week,
                 count(*) AS c
            RETURN year, week, c
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P84D')
            WITH date(c.posted_at).year AS year,
                 date(c.posted_at).week AS week,
                 count(*) AS c
            RETURN year, week, c
        }
        WITH t, year, week, sum(c) AS signals
        ORDER BY year, week
        WITH t,
             collect(signals) AS weekly,
             sum(signals) AS totalSignals12w,
             max(signals) AS peakWeek,
             count(signals) AS activeWeeks

        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WITH t, weekly, totalSignals12w, peakWeek, activeWeeks,
             min(p.posted_at) AS firstSeen,
             max(p.posted_at) AS lastSeen

        WITH t.name AS topic,
             weekly,
             totalSignals12w,
             peakWeek,
             activeWeeks,
             firstSeen,
             lastSeen,
             CASE WHEN size(weekly) >= 1 THEN weekly[size(weekly)-1] ELSE 0 END AS w0,
             CASE WHEN size(weekly) >= 2 THEN weekly[size(weekly)-2] ELSE 0 END AS w1,
             CASE WHEN size(weekly) >= 3 THEN weekly[size(weekly)-3] ELSE 0 END AS w2,
             CASE WHEN size(weekly) >= 4 THEN weekly[size(weekly)-4] ELSE 0 END AS w3,
             CASE WHEN size(weekly) >= 5 THEN weekly[size(weekly)-5] ELSE 0 END AS w4,
             CASE WHEN size(weekly) >= 6 THEN weekly[size(weekly)-6] ELSE 0 END AS w5,
             CASE WHEN size(weekly) >= 7 THEN weekly[size(weekly)-7] ELSE 0 END AS w6,
             CASE WHEN size(weekly) >= 8 THEN weekly[size(weekly)-8] ELSE 0 END AS w7

        WITH topic, weekly, totalSignals12w, peakWeek, activeWeeks, firstSeen, lastSeen,
             w0, w1, w2,
             toFloat(w0 + w1) / 2.0 AS avg2w,
             toFloat(w0 + w1 + w2 + w3) / 4.0 AS avg4w,
             toFloat(w4 + w5 + w6 + w7) / 4.0 AS avgPrev4w,
             duration.between(coalesce(firstSeen, datetime()), datetime()).days AS ageDays,
             CASE WHEN peakWeek > 0 THEN toFloat(w0) / toFloat(peakWeek) ELSE 0.0 END AS peakRatio,
             CASE
               WHEN activeWeeks > 0 THEN toFloat(activeWeeks) / 12.0
               ELSE 0.0
             END AS persistence,
             round(100.0 * (toFloat(w0 - w1) / toFloat(w1 + 3)), 1) AS wowPct,
             round(100.0 * ((toFloat(w0 + w1 + w2 + w3) / 4.0) - (toFloat(w4 + w5 + w6 + w7) / 4.0))
                         / ((toFloat(w4 + w5 + w6 + w7) / 4.0) + 3.0), 1) AS rollingGrowthPct,
             round(100.0 * (toFloat((w0 - w1) - (w1 - w2)) / (abs(toFloat(w1 - w2)) + 3.0)), 1) AS accelerationPct

        WITH topic, weekly, totalSignals12w, activeWeeks, firstSeen, lastSeen, ageDays,
             w0, w1, avg2w, avg4w, avgPrev4w, wowPct, rollingGrowthPct, accelerationPct,
             peakRatio, persistence,
             CASE
               WHEN totalSignals12w < 8 OR w0 = 0 THEN 'declining'
               WHEN (w0 - w1) >= 0
                    AND (
                      wowPct >= 10
                      OR rollingGrowthPct >= 12
                      OR (avg2w >= (avgPrev4w * 1.15) AND w0 >= 5)
                    )
                 THEN 'growing'
               ELSE 'declining'
             END AS stage,
             round(100.0 * (
                 0.5 * CASE WHEN totalSignals12w >= 80 THEN 1.0 ELSE toFloat(totalSignals12w) / 80.0 END +
                 0.3 * CASE WHEN activeWeeks >= 8 THEN 1.0 ELSE toFloat(activeWeeks) / 8.0 END +
                 0.2 * CASE WHEN abs(rollingGrowthPct) >= 80 THEN 1.0 ELSE abs(rollingGrowthPct) / 80.0 END
             ), 1) AS stageConfidence

        RETURN
            topic,
            stage,
            toString(firstSeen) AS firstSeen,
            toString(lastSeen) AS lastSeen,
            toInteger(ageDays) AS ageDays,
            toInteger(round(toFloat(ageDays) / 7.0)) AS ageWeeks,
            toInteger(totalSignals12w) AS totalSignals12w,
            toInteger(activeWeeks) AS activeWeeks,
            toInteger(w0) AS weeklyCurrent,
            toInteger(w1) AS weeklyPrev,
            toInteger(w0 - w1) AS weeklyDelta,
            round(avg2w, 1) AS avg2w,
            round(avg4w, 1) AS avg4w,
            round(avgPrev4w, 1) AS avgPrev4w,
            wowPct,
            rollingGrowthPct,
            accelerationPct,
            round(100.0 * persistence, 1) AS persistencePct,
            round(100.0 * peakRatio, 1) AS peakRatioPct,
            stageConfidence
        ORDER BY
            CASE stage
              WHEN 'growing' THEN 1
              WHEN 'declining' THEN 2
              ELSE 9
            END,
            weeklyCurrent DESC,
            stageConfidence DESC
    """)
