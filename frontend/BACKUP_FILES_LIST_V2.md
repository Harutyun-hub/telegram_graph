# BACKUP FILES LIST V2 — 2026-02-24
# Complete list of all project files and their status

## CORE APP FILES
| File | Status | Description |
|------|--------|-------------|
| /src/app/App.tsx | V2 stable | RouterProvider entry point |
| /src/app/routes.tsx | V2 stable | 6 routes: /, /posts, /graph, /achievements, /keywords, /settings |
| /src/app/layouts/AdminLayout.tsx | V2 stable | Sidebar nav + header + Outlet |

## PAGES
| File | Status | Description |
|------|--------|-------------|
| /src/app/pages/DashboardPage.tsx | V2 REBUILT | 8-tier collapsible, 32 community widgets |
| /src/app/pages/PostsPage.tsx | V1 (needs update) | Still political posts mock data |
| /src/app/pages/GraphPage.tsx | V1 stable | Force graph with filters, inspector |
| /src/app/pages/KeywordsPage.tsx | V1 (needs update) | Still political keywords |
| /src/app/pages/AchievementsPage.tsx | V1 (needs update) | Still Reddit gamification |
| /src/app/pages/SettingsPage.tsx | V1 stable | Profile, notifications, DB connections |

## WIDGET FILES (ALL V2 REBUILT)
| File | Widgets | Exports |
|------|---------|---------|
| /src/app/components/widgets/ExecutiveGlance.tsx | W1-W3 | CommunityHealthScore, TrendingTopicsFeed, CommunityBrief |
| /src/app/components/widgets/StrategicWidgets.tsx | W4-W7 | TopicLandscape, ConversationTrends, ContentEngagementHeatmap, QuestionCloud |
| /src/app/components/widgets/BehavioralWidgets.tsx | W8-W11 | ProblemTracker, ServiceGapDetector, SatisfactionByArea, MoodOverTime |
| /src/app/components/widgets/NetworkWidgets.tsx | W12,W13,W23-W25 | TopChannels, KeyVoices, ActivityTimeline, RecommendationTracker, NewcomerFlow |
| /src/app/components/widgets/PsychographicWidgets.tsx | W14-W18 | PersonaGallery, InterestRadar, OriginMap, IntegrationSpectrum, LocationDistribution |
| /src/app/components/widgets/PredictiveWidgets.tsx | W19-W22 | EmergingInterests, RetentionRiskGauge, CommunityGrowthFunnel, DecisionStageTracker |
| /src/app/components/widgets/ActionableWidgets.tsx | W26-W28 | BusinessOpportunityTracker, JobMarketPulse, HousingMarketPulse |
| /src/app/components/widgets/ComparativeWidgets.tsx | W29-W32 | WeekOverWeekShifts, SentimentByTopic, ContentPerformance, CommunityVitalityScorecard |

## SHARED COMPONENTS
| File | Status | Description |
|------|--------|-------------|
| /src/app/components/Logo.tsx | V1 stable | SVG logo (LogoIcon + Logo) |
| /src/app/components/GlobalFiltersLight.tsx | V1 stable | Filter sidebar for graph |
| /src/app/components/GraphVisualization.tsx | V1 stable | Force-directed graph |
| /src/app/components/NodeInspector.tsx | V1 stable | Node detail panel |
| /src/app/components/FloatingControls.tsx | V1 stable | Graph zoom controls |
| /src/app/components/GraphLegend.tsx | V1 stable | Graph color legend |
| /src/app/components/ExportButton.tsx | V1 stable | Export graph image |
| /src/app/components/AIQueryBar.tsx | V1 stable | AI query input |
| /src/app/components/ConnectionTest.tsx | V1 stable | DB connection test |
| /src/app/components/DateRangePicker.tsx | V1 stable | Date range picker |
| /src/app/components/DotMatrixBackground.tsx | V1 stable | Background pattern |
| /src/app/components/EmptyGraphState.tsx | V1 stable | Empty state |
| /src/app/components/SampleDataLoader.tsx | V1 stable | Sample data loader |
| /src/app/components/SettingsPanel.tsx | V1 stable | Settings panel |

## SERVICES & UTILS
| File | Status | Description |
|------|--------|-------------|
| /src/app/services/api.ts | V1 stable | Supabase edge function API |
| /src/app/utils/nodeColors.ts | V1 stable | 14 node type color palette |

## STYLES
| File | Status | Description |
|------|--------|-------------|
| /src/styles/theme.css | V2 (light bg) | CSS vars, #f9fafb background |
| /src/styles/fonts.css | Empty | No custom fonts |
| /src/styles/index.css | Stable | Import chain |
| /src/styles/tailwind.css | Stable | Tailwind base |

## UI COMPONENTS (shadcn - 47 files, all stable)
/src/app/components/ui/ — accordion, alert-dialog, alert, aspect-ratio, avatar, badge, breadcrumb, button, calendar, card, carousel, chart, checkbox, collapsible, command, context-menu, dialog, drawer, dropdown-menu, form, hover-card, input-otp, input, label, menubar, navigation-menu, pagination, popover, progress, radio-group, resizable, scroll-area, select, separator, sheet, sidebar, skeleton, slider, sonner, switch, table, tabs, textarea, toggle-group, toggle, tooltip, use-mobile.ts, utils.ts

## PROTECTED FILES (do not modify)
- /src/app/components/figma/ImageWithFallback.tsx

## BACKUP FILES
- /BACKUP_2026-02-24.md (V1 backup)
- /BACKUP_FILES_LIST.md (V1 file list)
- /BACKUP_2026-02-24_V2.md (THIS backup)
- /BACKUP_FILES_LIST_V2.md (THIS file list)

## REFERENCE DOCS
- /EXPERT_GRAPH_DATABASE_ANALYSIS.md
- /DATABASE_ANALYSIS_SUMMARY.md
- /GRAPH_DATABASE_MAPPING.md
- /CRITICAL_ANALYSIS.md
- /EXPERT_ANALYSIS.md
- /AI_AGENT_BRIEF.md
