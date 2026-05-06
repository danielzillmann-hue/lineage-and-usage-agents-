import { ResultsView } from "@/components/results/ResultsView";

export default async function ResultsPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return (
    <div className="mx-auto max-w-[1400px] px-6 py-8">
      <ResultsView runId={id} />
    </div>
  );
}
