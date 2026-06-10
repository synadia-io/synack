package main

import (
	"flag"
	"os"
	"time"

	"go.uber.org/zap/zapcore"
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

// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(natsv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		cpBaseURL            string
		reconcileInterval    time.Duration
		tokenEnv             string
		tokenFile            string
		timeout              time.Duration
		enableLeaderElection bool
		metricsAddr          string
		probeAddr            string
	)

	flag.StringVar(&cpBaseURL, "control-plane-base-url", "https://cloud.synadia.com", "API base URL, for example https://cloud.synadia.com")
	flag.DurationVar(&reconcileInterval, "reconcile-interval", time.Minute, "Interval between scheduled reconciliations for drift detection.")
	flag.StringVar(&tokenEnv, "token-var", "SYNACK_TOKEN", "Environment variable name for Control Plane token.")
	flag.StringVar(&tokenFile, "token-file", "", "File containing the Control Plane token. When set, this takes precedence over --token-var.")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "Timeout for Control Plane API requests.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")

	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	if opts.Development {
		opts.StacktraceLevel = zapcore.ErrorLevel
	} else {
		opts.StacktraceLevel = zapcore.PanicLevel
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "synack-controller-manager",
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	cpClient, err := controlplane.NewClient(controlplane.Options{
		BaseURL:   cpBaseURL,
		Timeout:   timeout,
		TokenEnv:  tokenEnv,
		TokenFile: tokenFile,
	})
	if err != nil {
		setupLog.Error(err, "unable to create control plane client")
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
	if err := (&controllers.TeamReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.TeamServiceAccountReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		ControlPlane:    cpClient,
		RequeueInterval: reconcileInterval,
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controllers.AppUserRoleBindingReconciler{
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
	if err := mgr.AddReadyzCheck("control-plane", newControlPlaneReadiness(cpClient, cpBaseURL, setupLog).Check); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}
