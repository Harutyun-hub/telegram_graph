export interface AdminWidgetSetting {
  enabled: boolean;
}

export interface AdminRuntimeConfig {
  openaiModel: string;
  questionBriefsModel: string;
  behavioralBriefsModel: string;
  questionBriefsPromptVersion: string;
  behavioralBriefsPromptVersion: string;
  aiPostPromptStyle: 'compact' | 'full';
  featureQuestionBriefsAi: boolean;
  featureBehavioralBriefsAi: boolean;
}

export interface AdminConfig {
  widgets: Record<string, AdminWidgetSetting>;
  prompts: Record<string, string>;
  promptDefaults?: Record<string, string>;
  runtime: AdminRuntimeConfig;
}

export interface AdminConfigPatch {
  widgets?: Record<string, AdminWidgetSetting>;
  prompts?: Record<string, string>;
  runtime?: Partial<AdminRuntimeConfig>;
}
