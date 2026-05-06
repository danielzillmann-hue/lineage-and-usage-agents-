import { LiveRun } from "@/components/run/LiveRun";

export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return (
    <div className="mx-auto max-w-[1400px] px-6 py-8">
      <LiveRun runId={id} />
    </div>
  );
}
