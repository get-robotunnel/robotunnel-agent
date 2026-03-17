use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionProfile {
    pub name: &'static str,
    pub summary: &'static str,
    pub defaults: ProjectionDefaults,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionDefaults {
    pub point_stride: Option<u32>,
    pub voxel_leaf_m: Option<f64>,
    pub image_scale: Option<f64>,
    pub hz_limit: Option<f64>,
    pub desired_delay_ms: u64,
}

pub fn builtin_profiles() -> Vec<ProjectionProfile> {
    vec![
        ProjectionProfile {
            name: "balanced",
            summary: "Balanced remote debug profile for mixed lidar/camera workloads.",
            defaults: ProjectionDefaults {
                point_stride: Some(4),
                voxel_leaf_m: Some(0.08),
                image_scale: Some(0.75),
                hz_limit: Some(8.0),
                desired_delay_ms: 80,
            },
        },
        ProjectionProfile {
            name: "lidar_low_bw",
            summary: "Aggressive point-cloud downsampling for narrow uplink environments.",
            defaults: ProjectionDefaults {
                point_stride: Some(10),
                voxel_leaf_m: Some(0.15),
                image_scale: Some(0.5),
                hz_limit: Some(5.0),
                desired_delay_ms: 150,
            },
        },
        ProjectionProfile {
            name: "vision_low_bw",
            summary: "Image-first debug profile with heavier image scaling and rate limits.",
            defaults: ProjectionDefaults {
                point_stride: Some(6),
                voxel_leaf_m: Some(0.1),
                image_scale: Some(0.4),
                hz_limit: Some(6.0),
                desired_delay_ms: 120,
            },
        },
        ProjectionProfile {
            name: "stats_only",
            summary:
                "Collect topic-level frequency, delay, and bandwidth without streaming payloads.",
            defaults: ProjectionDefaults {
                point_stride: None,
                voxel_leaf_m: None,
                image_scale: None,
                hz_limit: Some(2.0),
                desired_delay_ms: 200,
            },
        },
    ]
}
