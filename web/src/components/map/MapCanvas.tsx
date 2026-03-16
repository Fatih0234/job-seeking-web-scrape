"use client";

import "leaflet/dist/leaflet.css";
import L from "leaflet";
import { useEffect, useMemo, useRef } from "react";
import { CircleMarker, MapContainer, Marker, TileLayer, useMapEvents } from "react-leaflet";
import type { CityBubble, MapPoint } from "@/lib/apiTypes";

type BBox = { nelat: number; nelon: number; swlat: number; swlon: number };

function useDebouncedBounds(onBoundsChanged: (bbox: BBox) => void) {
  const tRef = useRef<number | null>(null);
  const map = useMapEvents({
    moveend() {
      if (tRef.current) window.clearTimeout(tRef.current);
      tRef.current = window.setTimeout(() => {
        const b = map.getBounds();
        onBoundsChanged({
          nelat: b.getNorthEast().lat,
          nelon: b.getNorthEast().lng,
          swlat: b.getSouthWest().lat,
          swlon: b.getSouthWest().lng,
        });
      }, 250);
    },
    zoomend() {
      const b = map.getBounds();
      onBoundsChanged({
        nelat: b.getNorthEast().lat,
        nelon: b.getNorthEast().lng,
        swlat: b.getSouthWest().lat,
        swlon: b.getSouthWest().lng,
      });
    },
  });
  return map;
}

function bubbleIcon(label: string, count: number) {
  const countText = `${count} Jobs`;
  const html = `
    <div class="gw-bubble-wrap">
      <div class="gw-bubble">${countText}</div>
      <div class="gw-bubble-label">${label}</div>
    </div>
  `;
  return L.divIcon({
    html,
    className: "gw-bubble-icon",
    iconSize: [1, 1],
    iconAnchor: [0, 0],
  });
}

export default function MapCanvas(props: {
  bubbles: CityBubble[];
  bubbleMetricField: "new_24h_unique" | "new_7d_unique" | "new_30d_unique" | "jobs_unique";
  remoteOn: boolean;
  remotePoints: MapPoint[];
  onBoundsChanged: (bbox: BBox) => void;
  onBubbleClicked: (b: CityBubble) => void;
}) {
  const metric = props.bubbleMetricField;

  const centered = useMemo(() => ({ lat: 51.1, lng: 10.4 }), []);

  // Initial bounds fetch
  const initialFetchDone = useRef(false);

  function BoundsWatcher() {
    const map = useDebouncedBounds(props.onBoundsChanged);
    useEffect(() => {
      if (initialFetchDone.current) return;
      initialFetchDone.current = true;
      const b = map.getBounds();
      props.onBoundsChanged({
        nelat: b.getNorthEast().lat,
        nelon: b.getNorthEast().lng,
        swlat: b.getSouthWest().lat,
        swlon: b.getSouthWest().lng,
      });
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
    return null;
  }

  return (
    <div className="h-full w-full bg-gray-100 dark:bg-gray-900">
      <MapContainer
        center={centered}
        zoom={6}
        minZoom={4}
        maxZoom={11}
        className="h-full w-full"
        zoomControl={false}
      >
        <BoundsWatcher />
        <TileLayer
          className="map-tiles"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {props.remoteOn
          ? props.remotePoints
              .filter((p) => p.lat != null && p.lon != null)
              .map((p) => (
                <CircleMarker
                  key={p.map_point_id}
                  center={[p.lat as number, p.lon as number]}
                  radius={5}
                  pathOptions={{
                    color: "#2563EB",
                    weight: 1,
                    opacity: 0.45,
                    fillColor: "#2563EB",
                    fillOpacity: 0.12,
                  }}
                />
              ))
          : null}

        {props.bubbles.map((b) => {
          const count =
            metric === "new_24h_unique"
              ? b.new_24h_unique
              : metric === "new_7d_unique"
                ? b.new_7d_unique
                : metric === "new_30d_unique"
                  ? (b.new_30d_unique ?? b.jobs_unique)
                  : b.jobs_unique;
          return (
            <Marker
              key={`${b.lat}:${b.lon}:${b.map_city_label}`}
              position={[b.lat, b.lon]}
              icon={bubbleIcon(b.map_city_label, count)}
              eventHandlers={{
                click: () => props.onBubbleClicked(b),
              }}
            />
          );
        })}
      </MapContainer>

      <style jsx global>{`
        .gw-bubble-icon {
          background: transparent;
          border: 0;
        }
        .gw-bubble-wrap {
          transform: translate(-50%, -50%);
          pointer-events: auto;
          display: grid;
          place-items: center;
          gap: 6px;
        }
        .gw-bubble {
          background: rgba(37, 99, 235, 0.92);
          color: white;
          border-radius: 9999px;
          padding: 4px 12px;
          font-weight: 700;
          font-size: 12px;
          border: 2px solid rgba(255, 255, 255, 0.9);
          box-shadow: 0 10px 18px -12px rgba(0, 0, 0, 0.5);
          white-space: nowrap;
        }
        .gw-bubble-label {
          font-size: 12px;
          font-weight: 800;
          color: rgba(17, 24, 39, 0.92);
          background: rgba(255, 255, 255, 0.82);
          border: 1px solid rgba(229, 231, 235, 0.9);
          padding: 2px 8px;
          border-radius: 9999px;
          backdrop-filter: blur(6px);
          white-space: nowrap;
        }
        .dark .gw-bubble-label {
          color: rgba(249, 250, 251, 0.92);
          background: rgba(0, 0, 0, 0.45);
          border-color: rgba(51, 65, 85, 0.9);
        }
      `}</style>
    </div>
  );
}
