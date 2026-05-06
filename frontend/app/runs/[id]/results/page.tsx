import { ResultsView } from "@/components/results/ResultsView";

export default async function ResultsPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <ResultsView runId={id} />;
}
