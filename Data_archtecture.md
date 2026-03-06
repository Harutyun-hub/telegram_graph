Product Design Brief
Armenian Telegram Intelligence Dashboard
Prepared by: Product Team
Date: February 24, 2026
Status: Ready for Design Review
Audience: UI/UX Design Team

1. Product Vision
We have built an AI-powered intelligence pipeline that monitors Armenian Telegram channels in real time, extracts deep behavioral and social insights from every comment, and stores them in a graph database.

The backend is fully operational. We now need a frontend that makes this intelligence readable and actionable for people who are not data scientists.

Core design challenge: We have one of the most sophisticated Telegram intelligence systems in existence — 19 AI-extracted dimensions per user, graph relationships across 12 entity types — and our users are politicians, NGO directors, and journalists who check their phones between meetings. The design must bridge that gap.

2. Target Users
Primary User — The Executive Monitor
Who: NGO director, political analyst, government advisor, journalist
Technical level: Comfortable with iPhone. Not comfortable with dashboards.
Time available: 30 seconds to 3 minutes
Core question: "Is something happening today that I should know about?"
Mental model: Morning news, not Excel
Secondary User — The Analyst
Who: Research assistant, communications strategist
Technical level: Medium — uses Google Sheets, reads reports
Time available: 5–15 minutes
Core question: "What is Channel X saying, and who is driving that conversation?"
Mental model: A detailed briefing document, not a raw database
Tertiary User — The Expert (future)
Who: Political scientist, intelligence specialist
Technical level: High
Time available: 30+ minutes
Core question: "What are the psychographic and ideological patterns in this audience?"
Mental model: A research tool
3. What the Backend Provides
The backend runs 24/7 and produces the following data signals (no setup needed from the design team — these all exist and are queryable):

3a. System-Wide Signals (all channels combined)
Signal	What it means	Example value
Comment volume today	How many comments were written	42
Volume vs. yesterday	Is today unusually active?	+340%
Dominant mood	How does the public feel right now	Anxious
Dominant geopolitical stance	Who are people aligning with	Pro-Armenian
Top topic	What is being discussed most	Border Security
Channel list	Which channels are monitored	@bagramyan26, @russianteaminarmenia
3b. Per-Channel Signals
Signal	What it means
Sentiment breakdown	% Optimistic, % Anxious, % Angry, etc.
Geopolitical breakdown	% Pro-Russian, % Pro-Western, % Pro-Armenian
Top topics	The 10 most discussed subjects in this channel
Activity by hour	When do people comment most (0–23h)
Most active users	Who is driving the conversation
User intent	Why are they commenting (venting, seeking info, agitating…)
3c. Deep Signals (expert use)
Signal	What it means
Migration intent	Are users signaling they want to leave the country?
Soviet nostalgia score	How nostalgic is this audience for the USSR? (0–1)
Collective memory references	Which historical events are being invoked (Genocide, Karabakh War…)
Trust in government / media	High / Medium / Low / Hostile
Business opportunity signals	People looking for jobs, partnerships, or market gaps
Daily life needs	Healthcare, housing, childcare complaints
Information warfare flags	Bot-like behavior, coordinated messaging
4. The 4-Floor UX Architecture
The interface is organized as progressive disclosure — like floors of a building. Users enter at Floor 0 and go deeper only if they need more.

Floor 0 — Daily Brief       ← Everyone lands here. 30 seconds.
    ↓ tap a channel
Floor 1 — Channel Dashboard  ← Analyst view. 5 minutes.
    ↓ tap "explore"
Floor 2 — Graph Explorer     ← Power user. 15 minutes.
    ↓ tap a node
Floor 3 — Expert Profile     ← Specialist. Deep dive.
Important: The graph explorer (Floor 2) is deprioritized for now. Focus all design energy on Floor 0 first, then Floor 1.

5. Floor 0 — Daily Brief
Purpose
Replace the user's morning news scroll. Answer one question in 30 seconds: "Is today normal?"

Design Requirements
Element	Content	Design Note
Status indicator	Green / Amber / Red based on whether today's volume is within normal range	Must be visible without scrolling. First thing the user sees.
AI-generated headline	A single 10–12 word sentence summarizing the day's dominant signal	Largest text on the page. Journalistic tone.
Context paragraph	2–3 sentences explaining the headline	Smaller, secondary. Reads like an analyst brief, not a report.
Topic chips	3–5 most discussed topics, sized by how dominant each is	Should feel like hashtags or news tags. Tappable.
Channel cards	One card per monitored channel. Shows: channel name + dominant mood + trending topic	Not a list of numbers. A story per channel.
Entry to Floor 1	Tapping a channel card → Floor 1	No visible "navigation." Just tap to go deeper.
Data freshness	"Last updated X minutes ago"	Small, under headline. Trust signal.
What NOT to Show on Floor 0
Individual user names or profiles
Raw comment counts without context (42 = meaningless. "+300% vs. yesterday" = meaningful)
Charts or graphs
Technical labels like "GeopoliticalStance: Pro_Armenia"
Mood/Sentiment Values (for color system)
The system produces these sentiment labels — the design needs a color for each:

Optimistic Hopeful Joyful Calm Factual Neutral Informative Reassuring Admiration Anxious Concern Critical Indignant Sad Defensive

Geopolitical Stance Values
Pro-Russian · Pro-Western · Pro-Armenian · Pro-Azerbaijani · Nationalist · Anti-Government · Neutral · Ambiguous

6. Floor 1 — Channel Dashboard
Purpose
Let an analyst understand one specific channel deeply: its mood, topics, and who is participating.

Design Requirements
Panel	Content	Chart type (suggestion)
Emotional Sentiment	Distribution of all sentiment labels for this channel	Donut or ring chart
Geopolitical Stance	Distribution of alignments	Horizontal bars
Top Topics	Top 10 topics by frequency	Horizontal bar chart
Activity by Hour	Comment density across 24 hours	Heatmap or area chart
Most Active Users	Top 5 users: name + intent role + geo-stance	Ranked list
What NOT to Show on Floor 1
Psychographic scores (nostalgia, trust, distress)
Collective memory details
Business opportunity signals
Raw Cypher queries or database IDs
7. Core UX Principles
These are non-negotiable design rules based on user research and competitive analysis:

IMPORTANT

Numbers need context. Never show a raw count without comparison. "42 comments" means nothing. "3× the daily average" is actionable.

IMPORTANT

Color is meaning. The same color must mean the same thing everywhere. If "Anxious" is red on Floor 0, it is red on Floor 1. Build a unified color vocabulary for the 15 sentiment values.

IMPORTANT

Above the fold = one answer. The user should answer their primary question without scrolling. On Floor 0: is today normal? On Floor 1: what is this channel's mood?

IMPORTANT

No jargon on the surface. "GeopoliticalStance: Pro_Armenia" is a database label. The user sees: "Pro-Armenian." "HAS_SENTIMENT → Anxious" becomes a red pill that says "Anxious."

NOTE

Mobile first. Many users check this during commutes. A vertical single-column layout for Floor 0 and Floor 1 should be the primary design.

NOTE

Competitive reference: Osavul (osavul.cloud) is the closest product in this space. They serve Ukrainian intelligence agencies. Their approach: narrative first, numbers second.

8. Visual Identity Constraints
Constraint	Detail
Theme	Dark background. The existing codebase uses #0b0e14 (near-black) with cyan accents.
Current accent	#00d4ff (cyan) for interactive elements
Typography	Inter or Outfit (already in use)
No emoji in professional context	Sentiment expressed through color + word, not emoji
No 3D charts, no pie charts with many slices	Keep it clean and readable
9. What We Need from the Design Team
Deliverables (Priority Order)
Floor 0 — Full screen mockup (desktop + mobile)

Status strip at top (green/amber/red)
AI headline hero section
Topic chips row
Channel cards (show 2–3 cards)
Propose the color vocabulary for sentiment labels
Floor 1 — Full screen mockup (desktop + mobile)

5 panels arranged for scannability
Propose chart choices for each panel
Show how a user navigates back to Floor 0
Color system proposal

One color per sentiment label (15 values)
One color per geopolitical stance (8 values)
Accessible contrast on dark background
Component library sketch

Status strip component
Channel card component
Topic chip component
User row component (for Floor 1)
Not Needed Yet
Floor 2 (graph explorer) — deferred
Floor 3 (expert mode) — deferred
Mobile app design — web browser only for now
10. Open Questions for Design Team
Should the status strip be a full-width banner at the very top, or an ambient color that changes the entire background tone?
How should channel cards handle the case where there's only 1 channel vs. 20 channels? (Scroll? Grid?)
The geopolitical stance has 8 values — some politically charged (Pro-Russian, Anti-Government). How do we display these neutrally without the UI appearing to take a political stance?
Should topic chips be tappable (→ filtered Floor 1 view) or purely informational on Floor 0?
What is the most intuitive way to show "3× normal activity" — a number, a bar, a pulse animation?