import { useEffect, useState } from 'react';
import { importApi } from '../api/imports';
import { metaApi } from '../api/meta';
import { EmptyState } from '../components/common/EmptyState';
import { GaugeMeter } from '../components/common/GaugeMeter';
import { SourceCard } from '../components/common/SourceCard';
import { StatusPill } from '../components/common/StatusPill';
import { TemplateTokenStrip } from '../components/common/TemplateTokenStrip';
import { sourceApi } from '../api/source';
import { useAuthStore } from '../stores/authStore';
import type { ImportBatch, MinePreview, Overview, RecomputeResult, Source } from '../types/models';
import { errorMessage } from '../utils/errorMessage';
import { formatCount } from '../utils/formatCount';
import { formatTime } from '../utils/formatTime';

const SAMPLE_HINT = '内置样本 249 行 = 240 行新格式（3 种格式 / 5 个形态）+ 9 行噪声分隔符，与 seed 的 240 条待归类事件逐字一致。';

export const ImportPage = () => {
  const can = useAuthStore((state) => state.can);
  const [text, setText] = useState('');
  const [preview, setPreview] = useState<MinePreview | null>(null);
  const [applied, setApplied] = useState<MinePreview | null>(null);
  const [sources, setSources] = useState<Source[]>([]);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [batches, setBatches] = useState<ImportBatch[]>([]);
  const [recompute, setRecompute] = useState<RecomputeResult | null>(null);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');

  const refresh = async () => {
    const [sourceList, summary, batchPage] = await Promise.all([
      sourceApi.list(),
      metaApi.overview(),
      importApi.batches(),
    ]);
    setSources(sourceList.items);
    setOverview(summary);
    setBatches(batchPage.items);
  };

  useEffect(() => {
    void refresh().catch((caught) => setError(errorMessage(caught, '加载失败')));
  }, []);

  const loadSample = async () => {
    setError('');
    setBusy('sample');
    try {
      const sample = await importApi.sample();
      setText(sample);
      setPreview(null);
      setApplied(null);
    } catch (caught) {
      setError(errorMessage(caught, '样本下载失败'));
    } finally {
      setBusy('');
    }
  };

  const runMine = async () => {
    setError('');
    setBusy('mine');
    try {
      const result = await importApi.mine(text);
      setPreview(result);
      setApplied(null);
    } catch (caught) {
      setError(errorMessage(caught, '挖掘失败'));
    } finally {
      setBusy('');
    }
  };

  const applyTemplates = async () => {
    setError('');
    setBusy('apply');
    try {
      const result = await importApi.apply(text);
      setApplied(result);
      setPreview(result);
      await refresh();
    } catch (caught) {
      setError(errorMessage(caught, '应用失败'));
    } finally {
      setBusy('');
    }
  };

  const runRecompute = async () => {
    setError('');
    setBusy('recompute');
    try {
      const result = await importApi.recompute();
      setRecompute(result);
      await refresh();
    } catch (caught) {
      setError(errorMessage(caught, '重算失败'));
    } finally {
      setBusy('');
    }
  };

  const shown = applied ?? preview;

  return (
    <>
      <header className="pagehead">
        <div>
          <div className="kicker">05 / INGEST &amp; MINE · Drain 挖掘</div>
          <h1>导入样本，挖出新模板</h1>
          <p className="lede">
            贴一份新服务的日志样本：先匹配库内已有模板，剩下的按 token 数分层 + 前缀树合并成新模板，
            差异位写成通配位并推断槽位类型。覆盖率 = 被「样本 ≥ 2」的模板覆盖的行 / 总行。
          </p>
        </div>
        <aside>
          <span>{SAMPLE_HINT}</span>
          <span>待归类事件 {overview ? formatCount(overview.pending_events) : '—'} 行</span>
        </aside>
      </header>
      <div className="pagebody">
        {error ? <div className="banner">{error}</div> : null}
        <div className="observation">
          <GaugeMeter label="样本行数" value={shown ? formatCount(shown.lines_total) : '—'} unit="行" sub="含空行与分隔符噪声" />
          <GaugeMeter label="覆盖率" value={shown ? shown.coverage_percent.toFixed(2) : '—'} unit="%" sub="被模板覆盖行 / 总行" accent />
          <GaugeMeter label="新模板" value={shown ? String(shown.new_templates) : '—'} unit="个" sub="样本 ≥ 2 才计入" />
          <GaugeMeter label="噪声行" value={shown ? String(shown.noise_lines) : '—'} unit="行" sub="归入 _noise，不参与覆盖率" />
        </div>

        <div className="split-asym">
          <section className="panel">
            <div className="panel-head">
              <h2>日志样本</h2>
              <span className="meta">{text.split('\n').filter((line) => line.length).length} 非空行</span>
            </div>
            <div className="panel-body">
              <div className="toolbar">
                <button className="btn" type="button" onClick={() => void loadSample()} disabled={busy !== ''}>
                  {busy === 'sample' ? '载入中…' : '载入内置样本'}
                </button>
                <button className="btn primary" type="button" onClick={() => void runMine()} disabled={!text || busy !== '' || !can('events:import')}>
                  {busy === 'mine' ? '挖掘中…' : '运行 Drain 挖掘'}
                </button>
                <button className="btn" type="button" onClick={() => void applyTemplates()} disabled={!shown || busy !== '' || !can('events:import')}>
                  {busy === 'apply' ? '应用中…' : '应用模板到事件'}
                </button>
              </div>
              <div className="field">
                <label>粘贴原始日志（每行一条）</label>
                <textarea
                  className="textarea"
                  style={{ minHeight: 320 }}
                  value={text}
                  onChange={(event) => setText(event.target.value)}
                  placeholder="2026-11-10T15:05:01.120 INFO order-svc.v2 handler=dispatch ..."
                  disabled={!can('events:import')}
                />
              </div>
              {!can('events:import') ? <p className="hint">当前角色无导入权限（403）。</p> : null}
            </div>
          </section>

          <section className="panel">
            <div className="panel-head">
              <h3>归属日志源</h3>
              <span className="meta">{sources.length} 个采集站</span>
            </div>
            <div className="side-list">
              {sources.map((source) => (
                <SourceCard key={source.id} source={source} compact />
              ))}
            </div>
          </section>
        </div>

        {applied ? (
          <div className={`banner ${applied.duplicate_import ? 'info' : 'ok'}`}>
            {applied.duplicate_import ? '同一批重复导入（指纹命中）：' : '应用完成：'}
            新建模板 {applied.created_templates ?? 0} 个 · 回填事件 {applied.assigned_events ?? 0} 条 · 新入库{' '}
            {applied.inserted_events ?? 0} 条 · 重复跳过 {applied.skipped_duplicates ?? 0} 条 · 重算{' '}
            {applied.recomputed_buckets ?? 0} 个模板桶 / {applied.metric_rows ?? 0} 行槽位指标 · 覆盖率{' '}
            {applied.coverage_percent.toFixed(2)}%
          </div>
        ) : null}
        {recompute ? (
          <div className={`banner ${recompute.violations === 0 ? 'ok' : ''}`}>
            全量重算完成：扫描 {formatCount(recompute.events_scanned)} 条已归类事件，重建{' '}
            {formatCount(recompute.metric_rows)} 行槽位指标，对齐违例 {recompute.violations} 条。
          </div>
        ) : null}

        <section className="panel">
          <div className="panel-head">
            <h2>挖掘结果</h2>
            <span className="meta">{shown ? `${shown.new_templates} 个新模板 · ${shown.known_hits} 行命中已有模板 · ${shown.elapsed_ms}ms` : '等待运行'}</span>
          </div>
          <div className="panel-body">
            {!shown ? (
              <EmptyState title="还没有挖掘结果" hint="先载入内置样本，再点「运行 Drain 挖掘」。" />
            ) : shown.templates.length === 0 ? (
              <EmptyState title="这一批没有挖出新模板" hint="样本可能都命中已有模板，或全是噪声行。" />
            ) : (
              <div className="stamp-seq">
                {shown.templates.map((template) => (
                  <div
                    key={template.signature}
                    style={{
                      border: '1px solid var(--rule)',
                      padding: '10px 12px',
                      display: 'grid',
                      gap: 6,
                      background: 'var(--paper)',
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'baseline' }}>
                      <TemplateTokenStrip
                        templateStr={template.template_str}
                        slotTypes={template.slot_types}
                        slotNames={template.slot_names}
                        highlight
                      />
                      <span className="mono" style={{ whiteSpace: 'nowrap', color: 'var(--ink-dim)' }}>
                        {template.sample_count} 行 · {template.token_count} token
                      </span>
                    </div>
                    <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
                      <StatusPill status={template.status === 'mined' ? 'mined' : 'tentative'} />
                      <span className="mono" style={{ color: 'var(--ink-dim)' }}>
                        归属 {template.suggested_source_name || '未匹配'} · level {template.level_hint.toUpperCase()}
                      </span>
                      <span className="mono truncate" style={{ color: 'var(--ink-dim)', maxWidth: '52ch' }} title={template.sample_line}>
                        {template.sample_line}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>结构化管道与导入批次</h2>
            <span className="meta">
              <button
                className="btn small ghost"
                type="button"
                disabled={busy !== '' || !can('events:import')}
                onClick={() => void runRecompute()}
              >
                {busy === 'recompute' ? '重算中…' : '全量重新计算槽位指标'}
              </button>
            </span>
          </div>
          <div className="panel-body flush">
            <p className="hint" style={{ padding: '12px 16px 0' }}>
              每条事件按模板通配位提取命名槽值（状态码、耗时等），按 (模板 × 5 分钟桶 × 槽位 × 槽值) 预聚合；
              同一批按全文指纹幂等，重复导入不新增事件、不重复计数；迟到记录归入新模板后回到原时间桶重算。
            </p>
            {batches.length === 0 ? (
              <EmptyState title="还没有导入批次" hint="应用一次样本后，这里会出现批次记录与重算口径。" />
            ) : (
              <table className="ruled">
                <thead>
                  <tr>
                    <th>批次指纹</th>
                    <th className="num">行数</th>
                    <th className="num">新入库</th>
                    <th className="num">重复跳过</th>
                    <th className="num">噪声</th>
                    <th className="num">新模板</th>
                    <th className="num">重算桶</th>
                    <th className="num">指标行</th>
                    <th>最近运行</th>
                  </tr>
                </thead>
                <tbody>
                  {batches.map((batch) => (
                    <tr key={batch.id}>
                      <td className="mono">{batch.content_fingerprint}…</td>
                      <td className="num">{formatCount(batch.rows_total)}</td>
                      <td className="num">{formatCount(batch.inserted_events)}</td>
                      <td className="num">{formatCount(batch.skipped_duplicates)}</td>
                      <td className="num">{formatCount(batch.noise_lines)}</td>
                      <td className="num">{formatCount(batch.created_templates)}</td>
                      <td className="num">{formatCount(batch.recomputed_buckets)}</td>
                      <td className="num">{formatCount(batch.metric_rows)}</td>
                      <td className="mono" style={{ whiteSpace: 'nowrap' }}>
                        {formatTime(batch.last_run_at)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </div>
    </>
  );
};
