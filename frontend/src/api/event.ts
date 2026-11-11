import { api, queryString } from './client';
import type { EventPage, SlotInvariantReport, SlotSeriesPayload, TemplateAggregate, TrendPayload } from '../types/models';

export const eventApi = {
  search: (params: {
    template_id?: number;
    source_id?: number;
    level?: string;
    search?: string;
    pending_only?: boolean;
    page?: number;
    page_size?: number;
  } = {}) => api.get<EventPage>(`/events${queryString(params)}`),
  trend: (buckets = 36, templateId?: number) => api.get<TrendPayload>(`/events/trend${queryString({ buckets, template_id: templateId })}`),
  aggregate: (top = 12) => api.get<{ items: TemplateAggregate[]; total: number }>(`/events/aggregate${queryString({ top })}`),
  levels: (templateId?: number) =>
    api.get<{ items: Array<{ level: string; count: number; ratio: number }> }>(`/events/levels${queryString({ template_id: templateId })}`),
  slots: (templateId: number, params: { buckets?: number; slot_index?: number; top_values?: number } = {}) =>
    api.get<SlotSeriesPayload>(`/events/slots${queryString({ template_id: templateId, ...params })}`),
  slotInvariants: (templateId?: number) =>
    api.get<SlotInvariantReport>(`/events/slots/invariants${queryString({ template_id: templateId })}`),
};
