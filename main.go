package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	natsv1alpha1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/controllers"
	"github.com/synadia-io/synack/internal/controlplane"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(natsv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		cpBaseURL            string
		reconcileInterval    time.Duration
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&cpBaseURL, "control-plane-base-url", "https://cloud.synadia.com", "API base URL, for example https://cloud.synadia.com")
	flag.DurationVar(&reconcileInterval, "reconcile-interval", time.Minute, "Interval between scheduled reconciliations for drift detection.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "synack-controller-manager",
	})
	if err != nil {
		os.Exit(1)
	}

	cpClient, err := controlplane.NewClient(controlplane.Options{
		BaseURL: cpBaseURL,
	})
	if err != nil {
		os.Exit(1)
	}

	if err := (&controllers.StreamReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.AccountReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.KeyValueReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.ObjectStoreReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.ConsumerReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.NatsUserReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}
