export function getSectionSum(sectionScores: Record<string, number>): number {
  return Object.values(sectionScores).reduce((a, b) => a + b, 0)
}
