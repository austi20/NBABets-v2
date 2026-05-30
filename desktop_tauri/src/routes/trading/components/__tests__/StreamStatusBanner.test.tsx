import { describe, it, expect, afterEach } from "vitest";
import { render, screen, cleanup } from "@testing-library/react";
import { StreamStatusBanner } from "../StreamStatusBanner";
import { useTradingStore } from "../../store";

afterEach(() => {
  cleanup();
  useTradingStore.setState({ streamConnected: true, streamError: null });
});

describe("StreamStatusBanner", () => {
  it("renders nothing when connected with no error", () => {
    useTradingStore.setState({ streamConnected: true, streamError: null });
    const { container } = render(<StreamStatusBanner />);
    expect(container).toBeEmptyDOMElement();
  });

  it("renders a disconnect indicator when the stream is disconnected", () => {
    useTradingStore.setState({ streamConnected: false, streamError: null });
    render(<StreamStatusBanner />);
    expect(screen.getByRole("status")).toHaveTextContent(/disconnected/i);
  });

  it("surfaces the streamError message when present", () => {
    useTradingStore.setState({
      streamConnected: true,
      streamError: { message: "Received an invalid trading snapshot; ignoring", at: 1 },
    });
    render(<StreamStatusBanner />);
    expect(screen.getByRole("status")).toHaveTextContent(/invalid trading snapshot/i);
  });
});
