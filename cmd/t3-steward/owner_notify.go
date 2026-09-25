package main

import (
	"context"
	"log/slog"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// ownerNotificationSinks builds the owner channels the configuration
// declares. The configuration was validated at load; a sink that fails here
// anyway (a webhook file removed since) is left out with an error line rather
// than stopping the coordinator, because notifications are a report about the
// fleet and never a reason to stop running it.
//
// The error is the configuration's own, which names the file and never the URL.
func ownerNotificationSinks(notifications config.Notifications, logger *slog.Logger) []ownernotify.Sink {
	var sinks []ownernotify.Sink
	if discord := notifications.Discord; discord != nil {
		events, eventsErr := ownernotify.ParseEvents(discord.Events)
		webhook, err := discord.WebhookURL()
		switch {
		case eventsErr != nil:
			logger.Error("discord owner notifications are disabled", "error", eventsErr)
		case err != nil:
			logger.Error("discord owner notifications are disabled", "error", err)
		default:
			sinks = append(sinks, ownernotify.NewDiscord(webhook, events, nil))
		}
	}
	if command := notifications.Command; command != nil {
		if events, err := ownernotify.ParseEvents(command.Events); err != nil {
			logger.Error("command owner notifications are disabled", "error", err)
		} else {
			sinks = append(sinks, ownernotify.NewCommand(command.Argv, events))
		}
	}
	return sinks
}

// startOwnerNotifier runs the owner-notification loop beside the coordinator's
// boundaries, on its own goroutine and its own ticker, so a webhook that is
// slow or down never delays scheduling. The returned function stops the loop
// and waits for it, and is safe to call when nothing was started.
func startOwnerNotifier(ctx context.Context, notifications config.Notifications, store ownernotify.Store, logger *slog.Logger) func() {
	sinks := ownerNotificationSinks(notifications, logger)
	if len(sinks) == 0 {
		return func() {}
	}
	names := make([]string, 0, len(sinks))
	for _, sink := range sinks {
		names = append(names, sink.Name())
	}
	logger.Info("owner notifications enabled", "sinks", names)
	notifier := &ownernotify.Notifier{Store: store, Sinks: sinks, Logger: logger.With("component", "owner-notify")}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		notifier.Run(loopCtx)
	}()
	return func() {
		cancel()
		<-done
	}
}
