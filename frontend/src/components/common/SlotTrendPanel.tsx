import { useEffect, useMemo, useState } from 'react';
import { eventApi } from '../../api/event';
import { EmptyState } from './EmptyState';
import { SLOT_LABELS } from '../../constants/enums';
import type { SlotSeriesPayload } from '../../types/models';
import { errorMessage } from '../../utils/errorMessage';
import { formatCount } from '../../utils/formatCount';
import { formatTime } from '../../utils/formatTime';

interface SlotTrendPanelProps {
  templateId: number | null;
  buckets?: number;
}

const UNIDENTIFIED = '__unidentified__';
const BAR_COLORS = ['var(--teal)', 'var(--amber-deep)', 'var(--ink-soft)', 'var(--brick)'];

/** 单个槽值分组的迷你柱状走势（纯 div，避免再引图表依赖）。 */
const MiniBars = ({ series, color }: { series: number[]; color: string }) => {
  const max = Math.max(1, ...series);
  return (
    <div className="slot-bars" aria-hidden>
      {series.map((value, index) => (
        <span
          key={index}
          className="slot-bar"
          style={{ height: `${Math.max(6, (value / max) * 100)}%`, background: value ? color : 'var(--rule)' }}
          title={`桶 ${index + 1}: ${value} 行`}
        />
      ))}
    </div>
  );
};

/** 槽位分组走势内容（不含 panel 外壳，由调用方放在 panel-body 内）。 */
export const SlotTrendPanel = ({ templateId, buckets = 24 }: SlotTrendPanelProps) => {
  const [data, setData] = useState<SlotSeriesPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (templateId === null) {
      setData(null);
      return;
    }
    setLoading(true);
    setError('');
    eventApi
      .slots(templateId, { buckets })
      .then(setData)
      .catch((caught) => setError(errorMessage(caught, '槽位走势加载失败')))
      .finally(() => setLoading(false));
  }, [templateId, buckets]);

  const rangeLabel = useMemo(() => {
    if (!data || data.buckets.length < 2) return '';
    return `${formatTime(data.buckets[0]).slice(5)} ~ ${formatTime(data.buckets[data.buckets.length - 1]).slice(5)}`;
  }, [data]);

  if (templateId === null) {
    return (
      <EmptyState title="未选择模板" hint="在上方模板排行里点一行，按状态码、耗时等命名槽位查看分组计数曲线。" />
    );
  }

  return (
    <div className="slot-trend">
      <div className="meta" style={{ textAlign: 'right' }}>
        {loading ? '加载中…' : data ? `${data.bucket_minutes} 分钟 / 桶 · ${rangeLabel}` : '等待数据'}
      </div>
      {error ? <div className="banner">{error}</div> : null}
      {!loading && data && data.slots.length === 0 ? (
        <EmptyState title="该模板还没有槽位指标" hint="先在导入页应用模板或发起一次全量重算。" />
      ) : null}
      {data?.slots.map((slot) => (
        <div className="slot-card" key={slot.slot_index}>
          <div className="slot-card-head">
            <span className="slot-card-title">
              <span className="mono">#{slot.slot_index}</span> {slot.slot_name}
              <em className="slot-type-tag">{SLOT_LABELS[slot.slot_type] ?? slot.slot_type}</em>
            </span>
            <span className="meta">
              {slot.unidentified_count > 0 ? (
                <span className="slot-unid-tag">未识别 {formatCount(slot.unidentified_count)} 行</span>
              ) : null}
            </span>
          </div>
          <table className="ruled slot-table">
            <thead>
              <tr>
                <th>槽值分组</th>
                <th>走势（{data.buckets.length} 桶）</th>
                <th className="num">合计</th>
              </tr>
            </thead>
            <tbody>
              {slot.groups.map((group, groupIndex) => (
                <tr key={group.slot_value} className={group.is_unidentified ? 'slot-row-unid' : ''}>
                  <td className="mono slot-value-cell" title={group.slot_value}>
                    {group.is_unidentified ? (
                      <span className="slot-unid-pill">未识别 / 缺值</span>
                    ) : (
                      group.slot_value
                    )}
                  </td>
                  <td style={{ minWidth: 220 }}>
                    <MiniBars series={group.series} color={BAR_COLORS[groupIndex % BAR_COLORS.length]} />
                  </td>
                  <td className="num">{formatCount(group.total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {slot.group_count > slot.shown_groups ? (
            <p className="hint" style={{ margin: '0 12px 8px' }}>
              该槽位共 {slot.group_count} 个取值，按总量展示前 {slot.shown_groups} 个（未识别分组始终保留）。
            </p>
          ) : null}
        </div>
      ))}
      {data && data.unidentified_total > 0 ? (
        <p className="hint">
          未识别分组（{UNIDENTIFIED}）汇总缺值、空值与无法归类的 token；每个模板桶里各槽位分组数量之和与该桶事件数
          × 槽位数严格对齐。
        </p>
      ) : null}
    </div>
  );
};
