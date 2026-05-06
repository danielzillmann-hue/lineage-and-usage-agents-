import { LiveRun } from "@/components/run/LiveRun";

export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
      <LiveRun runId={id} />
    </div>
  );
}
