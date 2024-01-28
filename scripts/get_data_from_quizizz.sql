WITH user_data AS (
    SELECT
    user_id,
    country
    FROM `quizizz-org`.`staging`.`user_stg`
    WHERE occupation != 'student'
    AND country IS NOT NULL
),

game_data AS (
    SELECT
    game_id,
    host_id,
    quiz_id,
    responses
    FROM raw.game_dim
    WHERE players > 1
    AND responses > 0
),

reported_quizzes AS (
    SELECT DISTINCT quiz_id
    FROM (
        SELECT
        id,
        MAX(CASE WHEN p.key = 'reason' THEN p.value.string_value END) AS reason,
        MAX(CASE WHEN p.key = 'quiz_id' THEN p.value.string_value END) AS quiz_id
        FROM event.reportEvents A,
        UNNEST(params) p
        WHERE eventName = 'ReportAbuse'
        GROUP BY 1
        
        UNION ALL

        SELECT 
        id,
        MAX(CASE WHEN p.key = 'hasError' THEN p.value.string_value END) AS has_error,
        MAX(CASE WHEN p.key = 'quiz_id' THEN p.value.string_value END) AS quiz_id
        FROM analytics_v2.feedback A,
        UNNEST(params) p
        WHERE eventName = 'ft_error_review'
        GROUP BY 1
        HAVING has_error = 'yes'
    )

    UNION ALL

    SELECT quiz_id AS quiz_id FROM clean.profanity GROUP BY 1
),

quiz_data AS (
    SELECT
    A.quiz_id,
    A.created_at,
    A.created_by,
    subject,
    quiz_language,
    country,
    grade_lower,
    grade_upper,
    A.quiz_type,
    is_premium,
    has_image
    FROM clean.quiz A
    INNER JOIN user_data B
    ON created_by = user_id
    AND NOT A.is_cloned
    AND is_public
    AND NOT is_deleted
    AND LENGTH(quiz_name) <= 100
    LEFT JOIN reported_quizzes E
    ON A.quiz_id = E.quiz_id
    AND E.quiz_id IS NULL
),

quiz_details AS (
    SELECT
    A.quiz_id,
    created_by,
    subject,
    quiz_language,
    country,
    CAST((grade_upper+grade_lower)/2 AS INT64) AS grade,
    is_premium,
    quiz_type,
    has_image,
    FROM quiz_data A
),

quiz_quality AS (SELECT
    quiz_id,
    ROW_NUMBER() OVER(ORDER BY recency_boost*image_boost*games*hosts) AS quiz_quality_score
    FROM (
        SELECT
        A.quiz_id,
        CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), B.created_at, DAY) < 365 THEN 2 ELSE 1 END AS recency_boost,
        CASE WHEN has_image = TRUE THEN 2 ELSE 1 END AS image_boost,
        COUNT(DISTINCT host_id) AS hosts,
        COUNT(DISTINCT game_id) AS games,
        COUNT(DISTINCT CASE WHEN B.country = C.country THEN host_id END) AS national_hosts,
        COUNT(DISTINCT CASE WHEN B.country = C.country THEN game_id END) AS national_games
        FROM game_data A
        INNER JOIN quiz_data B
        ON A.quiz_id = B.quiz_id
        INNER JOIN user_data C
        ON host_id = user_id
        GROUP BY 1,2,3
))

SELECT
  q.media.url AS image,
  STRING_AGG(DISTINCT questionText, ' ') AS question_text,
  STRING_AGG(DISTINCT option.text, ' ') AS option_text,
  STRING_AGG(DISTINCT quiz.quiz_name, ' ') AS quiz_name,
  MAX(q._id) AS questionId,
  SUM(quiz_quality_score) AS image_quality_score
FROM
  `analytics_v2.question3` AS q
LEFT JOIN
  `analytics_v2.quizQuestionLink2` AS qLink
ON
  q._id = qLink.questionId
LEFT JOIN
  `clean.quiz_version` AS qv
ON
  qLink.versionId = qv.version_id
LEFT JOIN
  `clean.quiz` AS quiz
ON
  quiz.quiz_id = qv.quiz_id,
  UNNEST(questionOptions) AS option
  INNER JOIN quiz_quality ON quiz_quality.quiz_id = quiz.quiz_id
WHERE
  quiz.is_deleted IS FALSE
  AND qv.is_cloned IS FALSE
  AND qv.is_draft IS FALSE
  AND (q.media.type = 'image' AND questionText is NOT NULL AND LENGTH(TRIM(questionText)) > 0 ) 
GROUP BY
  image