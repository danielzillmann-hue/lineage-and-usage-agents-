declare module "react-cytoscapejs" {
  import type { ComponentType, CSSProperties } from "react";
  import type cytoscape from "cytoscape";

  interface CytoscapeComponentProps {
    elements: cytoscape.ElementDefinition[];
    style?: CSSProperties;
    layout?: cytoscape.LayoutOptions;
    stylesheet?: cytoscape.Stylesheet[] | cytoscape.StylesheetCSS[] | unknown[];
    cy?: (cy: cytoscape.Core) => void;
    minZoom?: number;
    maxZoom?: number;
    wheelSensitivity?: number;
    pan?: cytoscape.Position;
    zoom?: number;
    autoungrabify?: boolean;
    autounselectify?: boolean;
    boxSelectionEnabled?: boolean;
    className?: string;
  }

  const CytoscapeComponent: ComponentType<CytoscapeComponentProps>;
  export default CytoscapeComponent;
}
